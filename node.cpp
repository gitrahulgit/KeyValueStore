#include <iostream>
#include <map>
#include <string>
#include <thread>
#include <vector>

struct Neighbour {
    std::string ip;
    int port;
    std::vector<int> keys;
};

class Node {
    std::map<int, std::string> localstore;
    std::vector<Neighbour> neighbours;
    int port;

public:
    Node() { this->port = 0; }
    Node(int port) { this->port = port; }
    ~Node() { exit(); }
    void start() {}
    void get(int key) {
        if (localstore.find(key) != localstore.end()) {
            std::cout << localstore[key] << std::endl;
            return;
        }
        for (Neighbour n : neighbours) {
            for (int k : n.keys) {
                if (k == key) {
                    // insert remote get logic here
                    std::cout << n.ip << ":" << n.port << std::endl;
                    return;
                }
            }
        }
        std::cout << "Key " << key << " not found" << std::endl;
    }
    void put();
    void del(int key) {
        if (localstore.find(key) != localstore.end()) {
            localstore.erase(key);
            std::cout << "Key " << key << " deleted" << std::endl;
        } else {
            // maybe give address of potential storer
            std::cout << "Key " << key << " does not exist in local store" << std::endl;
        }
    }
    void store() {
        if (localstore.size() == 0) {
            std::cout << "Local store is empty" << std::endl;
            return;
        }
        std::cout << "Local store:" << std::endl;
        for (std::pair<int, std::string> k : localstore) {
            std::cout << k.first << " " << k.second << std::endl;
        }
    }
    void exit();
};

int main(int argc, char *argv[]) {
    Node node(8080);
    node.start();
    return 0;
}