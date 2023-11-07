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

using namespace std;

#define BUFFER_SIZE 1024

string get_host_ip() {
    int sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd == -1) {
        cerr << "Error creating socket" << endl;
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
    return string(clientip);
}

struct Neighbour {
    string ip;
    int port;
    vector<int> keys;
};

class Store {
    map<int, string> store;
    mutex storemutex;

public:
    int put(int key, string value) {
        lock_guard<mutex> lock(storemutex);
        if (store.find(key) == store.end()) {
            store[key] = value;
            return 0;
        } else {
            store[key] = value;
            return 1;
        }
    }

    string get(int key) {
        lock_guard<mutex> lock(storemutex);
        if (store.find(key) == store.end()) {
            return "";
        }
        return store[key];
    }

    int del(int key) {
        lock_guard<mutex> lock(storemutex);
        if (store.find(key) == store.end()) {
            return 0;
        }
        store.erase(key);
        return 1;
    }

    int size() {
        lock_guard<mutex> lock(storemutex);
        return store.size();
    }

    void show() {
        lock_guard<mutex> lock(storemutex);
        for (pair<int, string> k : store) {
            cout << k.first << " " << k.second << endl;
        }
    }
};

class Node {
    Store localstore;
    vector<Neighbour> neighbours;
    mutex neighbourmutex;
    int port;
    string ip;
    string commMethod;
    vector<thread> serviceWorkerQueue;
    condition_variable initiateShutdown;
    bool shutdownQueued;
    mutex cvMutex;

    int sendMessage(string payload, bool receive, string &response, string ip, int port, int sockfd = -1) {
        if (commMethod == "tcp") {
            if (sockfd == -1) {
                sockfd = socket(AF_INET, SOCK_STREAM, 0);
                if (sockfd == -1) {
                    cerr << "Error creating socket" << endl;
                    return -1;
                }
            }
            struct sockaddr_in servaddr;
            servaddr.sin_family = AF_INET;
            servaddr.sin_port = htons(port);
            servaddr.sin_addr.s_addr = inet_addr(ip.c_str());
            if (connect(sockfd, (struct sockaddr *)&servaddr, sizeof(servaddr)) == -1) {
                close(sockfd);
                cerr << "Error connecting to " << ip << ":" << port << endl;
                return -1;
            }
            if (send(sockfd, payload.c_str(), payload.size(), 0) == -1) {
                close(sockfd);
                cerr << "Error sending payload" << endl;
                return -1;
            }
            if (receive) {
                vector<char> buf(BUFFER_SIZE);
                int recvsize = 0;
                do {
                    recvsize = recv(sockfd, &buf[0], buf.size(), 0);
                    if (recvsize == -1) {
                        close(sockfd);
                        cerr << "Error receiving response" << endl;
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
                    cerr << "Error creating socket" << endl;
                    return -1;
                }
            }
            struct sockaddr_in servaddr;
            servaddr.sin_family = AF_INET;
            servaddr.sin_port = htons(port);
            servaddr.sin_addr.s_addr = inet_addr(ip.c_str());
            if (sendto(sockfd, payload.c_str(), payload.size(), 0, (struct sockaddr *)&servaddr, sizeof(servaddr)) == -1) {
                close(sockfd);
                cerr << "Error sending payload" << endl;
                return -1;
            }
            if (receive) {
                vector<char> buf(BUFFER_SIZE);
                int recvsize = 0;
                do {
                    recvsize = recvfrom(sockfd, &buf[0], buf.size(), 0, NULL, 0);
                    if (recvsize == -1) {
                        close(sockfd);
                        cerr << "Error receiving response" << endl;
                        return -1;
                    }
                    response.append(buf.cbegin(), buf.cend());
                } while (recvsize == BUFFER_SIZE);
            }
            close(sockfd);
            return 0;
        } else {
            cerr << "RPC not implemented" << endl;
            return -1;
        }
    }
    void sendToAllNeighbours(string method, string payload, bool receive,
                             vector<string> &replies) { // should this wait for all threads?
        // create a thread for each neighbour and gather the reply into a vector
        vector<thread> threads;
        mutex replymutex;

        for (Neighbour n : neighbours) {
            threads.emplace_back([&replies, n, method, payload, receive, &replymutex, this]() {
                string response;
                sendMessage(payload, receive, response, n.ip, n.port);
                if (receive) {
                    lock_guard<mutex> lock(replymutex);
                    replies.push_back(response);
                }
            });
        }

        for (thread &t : threads) {
            t.join();
        }
    }
    int neighbourKeyCheck(int key) {
        lock_guard<mutex> lock(neighbourmutex);
        for (int i = 0; i < neighbours.size(); i++) {
            if (neighbours[i].keys.end() != find(neighbours[i].keys.begin(), neighbours[i].keys.end(), key)) {
                return i;
            }
        }
        return -1; // Return -1 if the key is not found
    }
    void neighbourGetKey(string method, int neighbourIndex, int key, string &response) {
        lock_guard<mutex> lock(neighbourmutex);
        sendMessage("get " + to_string(key), true, response, neighbours[neighbourIndex].ip, neighbours[neighbourIndex].port);
    }

    int neighbourGetSize() {
        lock_guard<mutex> lock(neighbourmutex);
        return neighbours.size();
    }

    void clientHandler(int clientfd, struct sockaddr *clientaddr, socklen_t clientaddrlen) {
        string request;
        vector<char> buf(BUFFER_SIZE);
        int recvsize = 0;
        do {
            recvsize = recv(clientfd, &buf[0], buf.size(), 0);
            if (recvsize == -1) {
                close(clientfd);
                cerr << "Error receiving request" << endl;
                return;
            }
            request.append(buf.cbegin(), buf.cend());
        } while (recvsize == BUFFER_SIZE);
        string value;
        int key;
        parseInput(request, key, value);
        transform(value.begin(), value.end(), value.begin(), ::toupper);
        if (request == "GET") {
            string value = localstore.get(key);
            if (!value.empty()) { // maybe use sendMessage with custom sockfd
                struct sockaddr_in *clin = (struct sockaddr_in *)clientaddr;
                char cip[INET_ADDRSTRLEN];
                int port = htons(clin->sin_port);
                inet_ntop(AF_INET, &(clin->sin_addr), cip, INET_ADDRSTRLEN);
                string ip = string(cip);
                string _empty;
                sendMessage(value, false, _empty, ip, port, clientfd);
            } else {
                cerr << ""; //?
            }
        } else if (request == "PUT") {

        } else if (request == "DEL") {

        } else if (request == "INIT") {
            // implement conflict resolution later
            // reply y
            struct sockaddr_in *clin = (struct sockaddr_in *)clientaddr;
            char cip[INET_ADDRSTRLEN];
            int port = htons(clin->sin_port);
            inet_ntop(AF_INET, &(clin->sin_addr), cip, INET_ADDRSTRLEN);
            string ip = string(cip);
            string _empty;
            sendMessage("y", false, _empty, ip, port, clientfd);
        }
        close(clientfd);
    }

    void startServer() {
        if (commMethod == "tcp") {
            thread t = thread([&]() {
                int sockfd = socket(AF_INET, SOCK_STREAM, 0);
                if (sockfd == -1) {
                    cerr << "Error creating socket" << endl;
                    return;
                }
                struct sockaddr_in servaddr;
                servaddr.sin_family = AF_INET;
                servaddr.sin_port = htons(port);
                servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
                if (bind(sockfd, (struct sockaddr *)&servaddr, sizeof(servaddr)) == -1) {
                    close(sockfd);
                    cerr << "Error binding socket" << endl;
                    return;
                }
                if (listen(sockfd, neighbourGetSize()) == -1) {
                    close(sockfd);
                    cerr << "Error listening" << endl;
                    return;
                }
                while (true) {
                    struct sockaddr_in clientaddr;
                    socklen_t clientaddrlen;
                    int clientfd = accept(sockfd, (struct sockaddr *)&clientaddr, &clientaddrlen);

                    if (clientfd == -1) {
                        close(sockfd);
                        cerr << "Error accepting connection" << endl;
                        return;
                    }
                    // PASS TO CLIENTHANDLER
                    // string request;
                    // vector<char> buf(BUFFER_SIZE);
                    // int recvsize = 0;
                    // do {
                    //     recvsize = recv(clientfd, &buf[0], buf.size(), 0);
                    //     if (recvsize == -1) {
                    //         close(sockfd);
                    //         cerr << "Error receiving request" << endl;
                    //         return;
                    //     }
                    //     request.append(buf.cbegin(), buf.cend());
                    // } while (recvsize == BUFFER_SIZE);
                    // string value;
                    // int key;
                    // parseInput(request, key, value);
                    // transform(value.begin(), value.end(), value.begin(), ::toupper);
                    // if (request == "GET") {
                    //     value = localstore.get(key);
                    //     if (storeCheck(key)) {
                    //         value = storeGet(key);
                    //     }
                    //     // send value to sender

                    // } else if (request == "PUT") {

                    // } else if (request == "DEL") {

                    // } else if (request == "INIT") {
                    // }
                    // string command = request.substr(0, request.find(" "));
                    // request = request.substr(request.find(" ") + 1);

                    // string response;
                    // sendMessage(request, true, response, inet_ntoa(clientaddr.sin_addr), ntohs(clientaddr.sin_port));
                    // if (send(clientfd, response.c_str(), response.size(), 0) == -1) {
                    //     close(sockfd);
                    //     cerr << "Error sending response" << endl;
                    //     return;
                    // }
                    // close(clientfd);
                }
            });
        } else if (commMethod == "udp") {
            // TODO
        } else {
            // TODO
        }
    }

public:
    Node(int port, string commMethod) {
        this->shutdownQueued = false;
        this->port = port; // dont need port anymore
        this->commMethod = commMethod;
        this->ip = get_host_ip();
        // open file addr.txt and read
        ifstream infile("../addr.txt"); // workaround for build dir
        if (!infile) {
            cerr << "Error opening file" << endl;
        }
        string line;
        while (getline(infile, line)) {
            string ip = line.substr(0, line.find(":"));
            if (ip == this->ip) {
                this->port = stoi(line.substr(line.find(":") + 1));
                continue;
            }
            int port = stoi(line.substr(line.find(":") + 1));
            neighbours.push_back({ip, port, {}});
        }
    }
    Node(int port) : Node(port, "tcp") {}
    Node(string commMethod) : Node(0, commMethod) {}
    Node() : Node(0, "tcp") {}
    void get(int key) {
        string value = localstore.get(key);
        if (!value.empty()) {
            cout << value << endl;
            return;
        }
        int neighbourWithKey = neighbourKeyCheck(key);
        if (neighbourWithKey == -1) {
            cout << "Key " << key << " not found" << endl;
            return;
        } else {
            string response;
            neighbourGetKey(commMethod, neighbourWithKey, key, response);
            cout << key << ": " << response << endl;
            return;
        }
    }
    void put(int key, string value) {
        string oldValue = localstore.get(key);
        if (!oldValue.empty() && localstore.put(key, value) == 1) {
            cout << "Key " << key << " updated" << endl;
        } else {
            localstore.del(key);
            vector<string> replies;
            sendToAllNeighbours(commMethod, "init " + to_string(key), true, replies);
            int ycount = 0;
            for (string reply : replies) {
                if (reply == "y") {
                    ycount++;
                }
            }
            if ((float)ycount > (float)neighbourGetSize() * 0.5 && localstore.put(key, value) == 0) {
                cout << "Key " << key << " created" << endl;
                vector<string> _empty = {};
                sendToAllNeighbours(commMethod, "put " + to_string(key), false, _empty);
            } else {
                cout << "Cannot update key: Conflict. Try again" << endl;
            }
        }
    };
    void del(int key) {

        if (localstore.del(key) == 1) {
            cout << "Key " << key << " deleted" << endl;
            // broadcast to all neighbours that value has been deleted
            vector<string> _empty = {};
            sendToAllNeighbours(commMethod, "del " + to_string(key), false, _empty);
        } else {
            // maybe give address of potential storer
            cout << "Key " << key << " does not exist in local store" << endl;
        }
    }
    void store() {
        if (localstore.size() == 0) {
            cout << "Local store is empty" << endl;
            return;
        }
        cout << "Local store:" << endl;
        localstore.show();
    }
    void exit() { // in progress
        unique_lock<mutex> lock(cvMutex);
        shutdownQueued = true;
        initiateShutdown.notify_all();
        lock.unlock();
        for (thread &t : serviceWorkerQueue) {
            t.join();
        }
    }; // cleanup

    void parseInput(string &action, int &key, string &value) {
        string skey;
        action.erase(0, action.find_first_not_of(" "));
        action.erase(find_if(action.rbegin(), action.rend(), [](int ch) { return !isspace(ch); }).base(), action.end());
        // split if there is a space for action and key
        if (action.find(" ") != string::npos) {
            skey = action.substr(action.find(" ") + 1);
            action = action.substr(0, action.find(" "));
            // split again if there is another space for key and value
            if (skey.find(" ") != string::npos) {
                value = skey.substr(skey.find(" ") + 1);
                skey = skey.substr(0, skey.find(" "));
            }
            key = stoi(skey);
        }
    }

    void start() {
        startServer();
        string action, value;
        int key;
        while (true) {
            getline(cin, action);
            parseInput(action, key, value);
            transform(action.begin(), action.end(), action.begin(), ::toupper);
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
                cout << "Invalid action" << endl;
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
