#include <algorithm>
#include <arpa/inet.h>
#include <condition_variable>
#include <deque>
#include <fstream>
#include <iostream>
#include <map>
#include <mutex>
// #include <netinet/in.h>
#include <netinet/in.h>
#include <string>
// #include <sys/socket.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

#define BUFFER_SIZE 1024

struct Neighbour {
    std::string ip;
    int port;
    std::vector<int> keys;
};

class Node {
    std::map<int, std::string> localstore;
    std::vector<Neighbour> neighbours;
    int port;
    std::string ip;
    std::string commMethod;
    std::mutex localmutex;
    std::mutex neighbourmutex;
    std::vector<std::thread> serviceWorkerQueue;
    std::condition_variable initiateShutdown;
    bool shutdownQueued;
    std::mutex cvMutex;

    std::string get_host_ip() {
        int sockfd = socket(AF_INET, SOCK_DGRAM, 0);
        if (sockfd == -1) {
            std::cerr << "Error creating socket" << std::endl;
            return "127.0.0.1";
        }
        struct sockaddr_in servaddr;
        servaddr.sin_addr.s_addr = inet_addr("10.254.254.254");
        servaddr.sin_family = AF_INET;
        servaddr.sin_port = htons(1);
        if (connect(sockfd, (struct sockaddr *)&servaddr, sizeof(servaddr)) == -1) {
            close(sockfd);
            return "127.0.0.1";
        }
        struct sockaddr_storage clientaddr;
        socklen_t clientaddrlen = sizeof(clientaddr);
        char clientip[INET_ADDRSTRLEN];
        if (getsockname(sockfd, (struct sockaddr *)&clientaddr, &clientaddrlen) == -1) {
            close(sockfd);
            return "127.0.0.1";
        }
        inet_ntop(AF_INET, &((struct sockaddr_in *)&clientaddr)->sin_addr, clientip, INET_ADDRSTRLEN);
        close(sockfd);
        return std::string(clientip);
    }

    void storeModify(int key, std::string value) {
        std::lock_guard<std::mutex> lock(localmutex);
        localstore[key] = value;
    }
    void storeDelete(int key) {
        std::lock_guard<std::mutex> lock(localmutex);
        localstore.erase(key);
    }
    int storeCheck(int key) {
        std::lock_guard<std::mutex> lock(localmutex);
        if (localstore.find(key) == localstore.end()) {
            return 0;
        }
        return 1;
    }
    int storeSize() {
        std::lock_guard<std::mutex> lock(localmutex);
        return localstore.size();
    }
    void storeShow() {
        std::lock_guard<std::mutex> lock(localmutex);
        for (std::pair<int, std::string> k : localstore) {
            std::cout << k.first << " " << k.second << std::endl;
        }
    }
    std::string storeGet(int key) {
        std::lock_guard<std::mutex> lock(localmutex);
        return localstore[key];
    }
    int sendMessage(std::string payload, bool receive, std::string &response, std::string ip,
                    int port, int sockfd = -1) {
        if (commMethod == "tcp") {
            if (sockfd == -1) {
                sockfd = socket(AF_INET, SOCK_STREAM, 0);
                if (sockfd == -1) {
                    std::cerr << "Error creating socket" << std::endl;
                    return -1;
                }
            }
            struct sockaddr_in servaddr;
            servaddr.sin_family = AF_INET;
            servaddr.sin_port = htons(port);
            servaddr.sin_addr.s_addr = inet_addr(ip.c_str());
            if (connect(sockfd, (struct sockaddr *)&servaddr, sizeof(servaddr)) == -1) {
                close(sockfd);
                std::cerr << "Error connecting to " << ip << ":" << port << std::endl;
                return -1;
            }
            if (send(sockfd, payload.c_str(), payload.size(), 0) == -1) {
                close(sockfd);
                std::cerr << "Error sending payload" << std::endl;
                return -1;
            }
            if (receive) {
                std::vector<char> buf(BUFFER_SIZE);
                int recvsize = 0;
                do {
                    recvsize = recv(sockfd, &buf[0], buf.size(), 0);
                    if (recvsize == -1) {
                        close(sockfd);
                        std::cerr << "Error receiving response" << std::endl;
                        return -1;
                    }
                    response.append(buf.cbegin(), buf.cend());
                } while (recvsize == BUFFER_SIZE);
            }
            close(sockfd);
            return 0;
        } else if (commMethod == "udp") {
            if (sockfd == -1) {
                sockfd = socket(AF_INET, SOCK_STREAM, 0);
                if (sockfd == -1) {
                    std::cerr << "Error creating socket" << std::endl;
                    return -1;
                }
            }
            struct sockaddr_in servaddr;
            servaddr.sin_family = AF_INET;
            servaddr.sin_port = htons(port);
            servaddr.sin_addr.s_addr = inet_addr(ip.c_str());
            if (sendto(sockfd, payload.c_str(), payload.size(), 0, (struct sockaddr *)&servaddr, sizeof(servaddr)) ==
                -1) {
                close(sockfd);
                std::cerr << "Error sending payload" << std::endl;
                return -1;
            }
            if (receive) {
                std::vector<char> buf(BUFFER_SIZE);
                int recvsize = 0;
                do {
                    recvsize = recvfrom(sockfd, &buf[0], buf.size(), 0, NULL, 0);
                    if (recvsize == -1) {
                        close(sockfd);
                        std::cerr << "Error receiving response" << std::endl;
                        return -1;
                    }
                    response.append(buf.cbegin(), buf.cend());
                } while (recvsize == BUFFER_SIZE);
            }
            close(sockfd);
            return 0;
        } else {
            std::cerr << "RPC not implemented" << std::endl;
            return -1;
        }
    }
    void sendToAllNeighbours(std::string method, std::string payload, bool receive, std::vector<std::string> &replies) {
        // create a thread for each neighbour and gather the reply into a vector
        std::vector<std::thread> threads;
        std::mutex replymutex;

        for (Neighbour n : neighbours) {
            threads.emplace_back([&replies, n, method, payload, receive, &replymutex, this]() {
                std::string response;
                sendMessage(payload, receive, response, n.ip, n.port);
                if (receive) {
                    std::lock_guard<std::mutex> lock(replymutex);
                    replies.push_back(response);
                }
            });
        }

        for (std::thread &t : threads) {
            t.join();
        }
    }
    int neighbourKeyCheck(int key) {
        std::lock_guard<std::mutex> lock(neighbourmutex);
        for (int i = 0; i < neighbours.size(); i++) {
            if (neighbours[i].keys.end() != std::find(neighbours[i].keys.begin(), neighbours[i].keys.end(), key)) {
                return i;
            }
        }
        return -1; // Return -1 if the key is not found
    }
    void neighbourGetKey(std::string method, int neighbourIndex, int key, std::string &response) {
        std::lock_guard<std::mutex> lock(neighbourmutex);
        sendMessage("get " + std::to_string(key), true, response, neighbours[neighbourIndex].ip,
                    neighbours[neighbourIndex].port);
    }

    int neighbourGetSize() {
        std::lock_guard<std::mutex> lock(neighbourmutex);
        return neighbours.size();
    }

    void clientHandler(int clientfd, struct sockaddr* clientaddr, socklen_t clientaddrlen) {
        std::string request;
        std::vector<char> buf(BUFFER_SIZE);
        int recvsize = 0;
        do {
            recvsize = recv(clientfd, &buf[0], buf.size(), 0);
            if (recvsize == -1) {
                close(clientfd);
                std::cerr << "Error receiving request" << std::endl;
                return;
            }
            request.append(buf.cbegin(), buf.cend());
        } while (recvsize == BUFFER_SIZE);
        std::string value;
        int key;
        parseInput(request, key, value);
        std::transform(value.begin(), value.end(), value.begin(), ::toupper);
        if (request == "GET") {
            if (storeCheck(key)) {// possible data race here
                value = storeGet(key);
                struct sockaddr_in* clin = (struct sockaddr_in *)clientaddr;
                char cip[INET_ADDRSTRLEN];
                int port = htons(clin->sin_port);
                inet_ntop(AF_INET, &(clin->sin_addr), cip, INET_ADDRSTRLEN);
                std::string ip = std::string(cip);
                std::string _empty;
                sendMessage(value, false, _empty, ip, port);
            } else {
                std::cerr << "";
            }
        } else if (request == "PUT") {

        } else if (request == "DEL") {

        } else if (request == "INIT") {

        }
        close(clientfd);
    }

    void startServer() {
        if (commMethod == "tcp") {
            std::thread t = std::thread([&]() {
                int sockfd = socket(AF_INET, SOCK_STREAM, 0);
                if (sockfd == -1) {
                    std::cerr << "Error creating socket" << std::endl;
                    return;
                }
                struct sockaddr_in servaddr;
                servaddr.sin_family = AF_INET;
                servaddr.sin_port = htons(port);
                servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
                if (bind(sockfd, (struct sockaddr *)&servaddr, sizeof(servaddr)) == -1) {
                    close(sockfd);
                    std::cerr << "Error binding socket" << std::endl;
                    return;
                }
                if (listen(sockfd, neighbourGetSize()) == -1) {
                    close(sockfd);
                    std::cerr << "Error listening" << std::endl;
                    return;
                }
                while (true) {
                    struct sockaddr_in clientaddr;
                    socklen_t clientaddrlen;
                    int clientfd = accept(sockfd, (struct sockaddr *)&clientaddr, &clientaddrlen);

                    if (clientfd == -1) {
                        close(sockfd);
                        std::cerr << "Error accepting connection" << std::endl;
                        return;
                    }
                    std::string request;
                    std::vector<char> buf(BUFFER_SIZE);
                    int recvsize = 0;
                    do {
                        recvsize = recv(clientfd, &buf[0], buf.size(), 0);
                        if (recvsize == -1) {
                            close(sockfd);
                            std::cerr << "Error receiving request" << std::endl;
                            return;
                        }
                        request.append(buf.cbegin(), buf.cend());
                    } while (recvsize == BUFFER_SIZE);
                    std::string value;
                    int key;
                    parseInput(request, key, value);
                    std::transform(value.begin(), value.end(), value.begin(), ::toupper);
                    if (request == "GET") {
                        if (storeCheck(key)) {
                            value = storeGet(key);
                        }
                        // send value to sender

                    } else if (request == "PUT") {

                    } else if (request == "DEL") {

                    } else if (request == "INIT") {
                    }
                    std::string command = request.substr(0, request.find(" "));
                    request = request.substr(request.find(" ") + 1);

                    std::string response;
                    sendMessage(request, true, response, inet_ntoa(clientaddr.sin_addr),
                                ntohs(clientaddr.sin_port));
                    if (send(clientfd, response.c_str(), response.size(), 0) == -1) {
                        close(sockfd);
                        std::cerr << "Error sending response" << std::endl;
                        return;
                    }
                    close(clientfd);
                }
            });
        } else if (commMethod == "udp") {
            // TODO
        } else {
            // TODO
        }
    }

public:
    Node(int port, std::string commMethod) {
        this->shutdownQueued = false;
        this->port = port;
        this->commMethod = commMethod;
        this->ip = get_host_ip();
        // open file addr.txt and read
        std::ifstream infile("../addr.txt"); // workaround for build dir
        if (!infile) {
            std::cerr << "Error opening file" << std::endl;
        }
        std::string line;
        while (std::getline(infile, line)) {
            std::string ip = line.substr(0, line.find(":"));
            if (ip == this->ip) {
                continue;
            }
            int port = std::stoi(line.substr(line.find(":") + 1));
            neighbours.push_back({ip, port, {}});
        }
    }
    Node(int port) : Node(port, "tcp") {}
    Node(std::string commMethod) : Node(0, commMethod) {}
    Node() : Node(0, "tcp") {}
    void get(int key) {
        if (storeCheck(key)) {
            std::cout << storeGet(key) << std::endl;
            return;
        }
        int neighbourWithKey = neighbourKeyCheck(key);
        if (neighbourWithKey == -1) {
            std::cout << "Key " << key << " not found" << std::endl;
            return;
        } else {
            std::string response;
            neighbourGetKey(commMethod, neighbourWithKey, key, response);
            std::cout << key << ": " << response << std::endl;
            return;
        }
    }
    void put(int key, std::string value) {
        if (storeCheck(key)) {
            storeModify(key, value);
            std::cout << "Key " << key << " updated" << std::endl;
        } else {
            std::vector<std::string> replies;
            sendToAllNeighbours(commMethod, "init " + std::to_string(key), true, replies);
            int ycount = 0;
            for (std::string reply : replies) {
                if (reply == "y") {
                    ycount++;
                }
            }
            if ((float)ycount > (float)neighbourGetSize() * 0.5) {
                storeModify(key, value);
                std::cout << "Key " << key << " created" << std::endl;
                std::vector<std::string> _empty = {};
                sendToAllNeighbours(commMethod, "put " + std::to_string(key), false, _empty);
            } else {
                std::cout << "Cannot update key: Conflict. Try again" << std::endl;
            }
        }
    };
    void del(int key) {
        if (storeCheck(key)) {
            storeDelete(key);
            std::cout << "Key " << key << " deleted" << std::endl;
            // broadcast to all neighbours that value has been deleted
            std::vector<std::string> _empty = {};
            sendToAllNeighbours(commMethod, "del " + std::to_string(key), false, _empty);
        } else {
            // maybe give address of potential storer
            std::cout << "Key " << key << " does not exist in local store" << std::endl;
        }
    }
    void store() {
        if (storeSize() == 0) {
            std::cout << "Local store is empty" << std::endl;
            return;
        }
        std::cout << "Local store:" << std::endl;
        storeShow();
    }
    void exit() {
        std::unique_lock<std::mutex> lock(cvMutex);
        shutdownQueued = true;
        initiateShutdown.notify_all();
        lock.unlock();
        for (std::thread &t : serviceWorkerQueue) {
            t.join();
        }
    }; // cleanup

    void parseInput(std::string &action, int &key, std::string &value) {
        std::string skey;
        action.erase(0, action.find_first_not_of(" "));
        action.erase(std::find_if(action.rbegin(), action.rend(), [](int ch) { return !std::isspace(ch); }).base(),
                     action.end());
        // split if there is a space for action and key
        if (action.find(" ") != std::string::npos) {
            skey = action.substr(action.find(" ") + 1);
            action = action.substr(0, action.find(" "));
            // split again if there is another space for key and value
            if (skey.find(" ") != std::string::npos) {
                value = skey.substr(skey.find(" ") + 1);
                skey = skey.substr(0, skey.find(" "));
            }
            key = std::stoi(skey);
        }
    }

    void start() {
        startServer();
        std::string action, value;
        int key;
        while (true) {
            std::getline(std::cin, action);
            parseInput(action, key, value);
            std::transform(action.begin(), action.end(), action.begin(), ::toupper);
            if (action == "GET") {
                get(key);
            } else if (action == "PUT") {
                put(key, value);
            } else if (action == "DELETE") {
                del(key);
            } else if (action == "STORE") {
                store();
            } else if (action == "EXIT") {
                exit();
                break;
            } else {
                std::cout << "Invalid action" << std::endl;
            }
        }
    }
    ~Node() {}
};

int main(int argc, char *argv[]) {
    Node node(25621);
    node.start();
    return 0;
}
