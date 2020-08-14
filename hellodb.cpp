#include <iostream>
#include <fstream>
#include <string>
#include <cstring>
#include <vector>
#include <map>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <signal.h>
#include <wiredtiger.h>

char empty_string[1] = {};

std::map<std::string,std::vector<std::string>> table_to_meta;
std::map<std::string,std::vector<std::string>> index_to_meta;
std::map<std::string,std::map<std::string,int>> table_to_field_to_num;

int customWrite(int socket, const char* buffer, int length) {
    int ret = write(socket, buffer, length);
    if(ret < 0) {
        std::cerr << "broken pipe" << std::endl;
    }
    return ret;
}

int createTables(WT_SESSION* session) {
    int ret = 0;
    std::string config;
    
    for(auto it=table_to_meta.begin(); it != table_to_meta.end(); ++it) {
        config = "key_format=r,value_format=";
        
        int nfields = it->second.size();
        for(int i=0; i < nfields; ++i) {
            config += 'S';
        }
        
        config += ",columns=(id,";
        for(int i=0; i < nfields; ++i) {
            config += it->second.at(i);
            config += ',';
        }
        config.pop_back();
        config += ')';
        
        if(ret = session->create(session, it->first.c_str(), config.c_str()) != 0) {
            std::cerr << wiredtiger_strerror(ret) << std::endl;
            return ret;
        }
    }
    
    return ret;
}

int createIndexes(WT_SESSION* session) {
    int ret = 0;
    std::string config;
    
    for(auto it=index_to_meta.begin(); it != index_to_meta.end(); ++it) {
        config = "columns=(";
        
        int nfields = it->second.size();
        for(int i=0; i < nfields; ++i) {
            config += it->second.at(i);
            config += ',';
        }
        config.pop_back();
        config += ')';
        
        if(ret = session->create(session, it->first.c_str(), config.c_str()) != 0)  {
            std::cerr << wiredtiger_strerror(ret) << std::endl;
            return ret;
        }
    }
    
    return ret;
}

void createCursors(WT_SESSION* session, std::map<std::string,WT_CURSOR*> &cursor_pool) {
    WT_CURSOR* cursor;
    
    for(auto it=table_to_meta.begin(); it != table_to_meta.end(); ++it) {
        session->open_cursor(session, it->first.c_str(), NULL, "append,overwrite=false", &cursor);
        cursor_pool[it->first] = cursor;
    }
    for(auto it=index_to_meta.begin(); it != index_to_meta.end(); ++it) {
        std::string wiredtiger_table_projection(it->first);
        wiredtiger_table_projection += "(id,";
        
        std::string wiredtiger_table("table:");
        wiredtiger_table += it->first.substr(6, it->first.find(':', 6)-6); //index:testing:name
        int nfields = table_to_meta[wiredtiger_table].size();
        for(int i=0; i < nfields; ++i) {
            wiredtiger_table_projection += table_to_meta[wiredtiger_table].at(i);
            wiredtiger_table_projection += ',';
        }
        wiredtiger_table_projection.pop_back();
        wiredtiger_table_projection += ')';
        
        session->open_cursor(session, wiredtiger_table_projection.c_str(), NULL, "append,overwrite=false", &cursor);
        cursor_pool[it->first] = cursor;
    }
}

void parseBuffer(char* buffer_read, int bytes_read, std::vector<char*> &tokens) {
    for(int i=0; i < bytes_read; ++i) {
        if(buffer_read[i] == ']' || buffer_read[i] == 10 || buffer_read[i] == 13) {
            if(buffer_read[i] == ']') {
                tokens.push_back(buffer_read+i+1);
            }
            buffer_read[i] = 0;
        }
    }
}

void insert(std::map<std::string,WT_CURSOR*> &cursor_pool, std::vector<char*> &tokens, int socket_client) {
    std::string wiredtiger_table("table:");
    wiredtiger_table += tokens.at(0);
    
    if(table_to_meta.count(wiredtiger_table) == 0) {
        std::string tmp("\nTable does not exist\n\n");
        customWrite(socket_client, tmp.c_str(), tmp.length());
        return;
    }
    
    int nfields = table_to_meta[wiredtiger_table].size();
    while(tokens.size()-1 < nfields) {
        tokens.push_back(empty_string);
    }
    
    WT_CURSOR* cursor = cursor_pool[wiredtiger_table];
    switch(nfields) {
        case 1:
            cursor->set_value(cursor, tokens.at(1));
            break;
        case 2:
            cursor->set_value(cursor, tokens.at(1), tokens.at(2));
            break;
        case 3:
            cursor->set_value(cursor, tokens.at(1), tokens.at(2), tokens.at(3));
            break;
        case 4:
            cursor->set_value(cursor, tokens.at(1), tokens.at(2), tokens.at(3), tokens.at(4));
            break;
        case 5:
            cursor->set_value(cursor, tokens.at(1), tokens.at(2), tokens.at(3), tokens.at(4), tokens.at(5));
            break;
        case 6:
            cursor->set_value(cursor, tokens.at(1), tokens.at(2), tokens.at(3), tokens.at(4), tokens.at(5), tokens.at(6));
            break;
        case 7:
            cursor->set_value(cursor, tokens.at(1), tokens.at(2), tokens.at(3), tokens.at(4), tokens.at(5), tokens.at(6), tokens.at(7));
            break;
    }
    
    if(cursor->insert(cursor) == 0) {
        uint64_t recno;
        cursor->get_key(cursor, &recno);
        char recno_str[21];
        sprintf(recno_str, "%lu", recno);
        std::string buffer_write = "\ninserted>\nrecno: ";
        buffer_write += recno_str;
        buffer_write += "\n\n";
        customWrite(socket_client, buffer_write.c_str(), buffer_write.length());
    }
    else {
        customWrite(socket_client, "\nInsert failed\n\n", 16);
    }
}

void at(std::map<std::string,WT_CURSOR*> &cursor_pool, std::vector<char*> &tokens, int socket_client) {
    if(strcmp(tokens.at(1), "0") == 0) {
        std::string tmp("\nNot found\n\n");
        customWrite(socket_client, tmp.c_str(), tmp.length());
        return;
    }
    
    std::string wiredtiger_table("table:");
    wiredtiger_table += tokens.at(0);
    
    if(table_to_meta.count(wiredtiger_table) == 0) {
        std::string tmp("\nTable does not exist\n\n");
        customWrite(socket_client, tmp.c_str(), tmp.length());
        return;
    }
    
    WT_CURSOR* cursor = cursor_pool[wiredtiger_table];
    cursor->set_key(cursor, strtoul(tokens.at(1), NULL, 0));
    
    int nfields = table_to_meta[wiredtiger_table].size();
    char* fields[nfields];
    if(cursor->search(cursor) == 0) {
        switch(nfields) {
            case 1:
                cursor->get_value(cursor, &fields[0]);
                break;
            case 2:
                cursor->get_value(cursor, &fields[0], &fields[1]);
                break;
            case 3:
                cursor->get_value(cursor, &fields[0], &fields[1], &fields[2]);
                break;
            case 4:
                cursor->get_value(cursor, &fields[0], &fields[1], &fields[2], &fields[3]);
                break;
            case 5:
                cursor->get_value(cursor, &fields[0], &fields[1], &fields[2], &fields[3], &fields[4]);
                break;
            case 6:
                cursor->get_value(cursor, &fields[0], &fields[1], &fields[2], &fields[3], &fields[4], &fields[5]);
                break;
            case 7:
                cursor->get_value(cursor, &fields[0], &fields[1], &fields[2], &fields[3], &fields[4], &fields[5], &fields[6]);
                break;
        }
        
        std::string buffer_write = "\n";
        buffer_write += tokens.at(1);
        buffer_write += ">\n";
        for(int i=0; i < nfields; ++i) {
            buffer_write += table_to_meta[wiredtiger_table].at(i);
            buffer_write += ": ";
            buffer_write += fields[i];
            buffer_write += '\n';
        } 
        buffer_write += '\n';
        customWrite(socket_client, buffer_write.c_str(), buffer_write.length());
    }
    else {
        std::string tmp("\nNot found\n\n");
        customWrite(socket_client, tmp.c_str(), tmp.length());
    }
    
    cursor->reset(cursor);
}

void dump(std::map<std::string,WT_CURSOR*> &cursor_pool, std::vector<char*> &tokens, int socket_client) {
    std::string wiredtiger_table("table:");
    wiredtiger_table += tokens.at(0);
    
    if(table_to_meta.count(wiredtiger_table) == 0) {
        std::string tmp("\nTable does not exist\n\n");
        customWrite(socket_client, tmp.c_str(), tmp.length());
        return;
    }
    
    WT_CURSOR* cursor = cursor_pool[wiredtiger_table];
    int nfields = table_to_meta[wiredtiger_table].size();
    char* fields[nfields];
    uint64_t recno;
    char recno_str[21];
    while(cursor->next(cursor) == 0) {
        switch(nfields) {
            case 1:
                cursor->get_value(cursor, &fields[0]);
                break;
            case 2:
                cursor->get_value(cursor, &fields[0], &fields[1]);
                break;
            case 3:
                cursor->get_value(cursor, &fields[0], &fields[1], &fields[2]);
                break;
            case 4:
                cursor->get_value(cursor, &fields[0], &fields[1], &fields[2], &fields[3]);
                break;
            case 5:
                cursor->get_value(cursor, &fields[0], &fields[1], &fields[2], &fields[3], &fields[4]);
                break;
            case 6:
                cursor->get_value(cursor, &fields[0], &fields[1], &fields[2], &fields[3], &fields[4], &fields[5]);
                break;
            case 7:
                cursor->get_value(cursor, &fields[0], &fields[1], &fields[2], &fields[3], &fields[4], &fields[5], &fields[6]);
                break;
        }

        cursor->get_key(cursor, &recno);
        sprintf(recno_str, "%lu", recno);

        std::string buffer_write("\n");
        buffer_write += recno_str;
        buffer_write += ">\n";
        for(int i=0; i < nfields; ++i) {
            buffer_write += table_to_meta[wiredtiger_table].at(i);
            buffer_write += ": ";
            buffer_write += fields[i];
            buffer_write += '\n';
        }
        if(customWrite(socket_client, buffer_write.c_str(), buffer_write.length()) < 0) {
            break;
        }
    }
    customWrite(socket_client, "\n", 1);
    cursor->reset(cursor);
}

void find(std::map<std::string,WT_CURSOR*> &cursor_pool, std::vector<char*> &tokens, int socket_client) {
    std::string wiredtiger_table("index:");
    wiredtiger_table += tokens.at(0);
    wiredtiger_table += ':';
    wiredtiger_table += tokens.at(1);
    
    if(index_to_meta.count(wiredtiger_table) == 0) {
        std::string tmp("\nTable or index does not exist\n\n");
        customWrite(socket_client, tmp.c_str(), tmp.length());
        return;
    }

    int index_nfields = index_to_meta[wiredtiger_table].size();
    while(tokens.size()-2 < index_nfields) {
        tokens.push_back(empty_string);
    }

    WT_CURSOR* cursor = cursor_pool[wiredtiger_table];
    switch(index_nfields) {
        case 1:
            cursor->set_key(cursor, tokens.at(2));
            break;
        case 2:
            cursor->set_key(cursor, tokens.at(2), tokens.at(3));
            break;
        case 3:
            cursor->set_key(cursor, tokens.at(2), tokens.at(3), tokens.at(4));
            break;
        case 4:
            cursor->set_key(cursor, tokens.at(2), tokens.at(3), tokens.at(4), tokens.at(5));
            break;
        case 5:
            cursor->set_key(cursor, tokens.at(2), tokens.at(3), tokens.at(4), tokens.at(5), tokens.at(6));
            break;
        case 6:
            cursor->set_key(cursor, tokens.at(2), tokens.at(3), tokens.at(4), tokens.at(5), tokens.at(6), tokens.at(7));
            break;
        case 7:
            cursor->set_key(cursor, tokens.at(2), tokens.at(3), tokens.at(4), tokens.at(5), tokens.at(6), tokens.at(7), tokens.at(8));
            break;
    }

    if(cursor->search(cursor) != 0) {
        std::string tmp("\nNot found\n\n");
        customWrite(socket_client, tmp.c_str(), tmp.length());
        return;
    }

    std::string wiredtiger_table2 = "table:";
    wiredtiger_table2 += tokens.at(0);
    int nfields = table_to_meta[wiredtiger_table2].size();
    uint64_t recno;
    char* fields[nfields];
    bool match;
    char recno_str[21];
    do {
        switch(nfields) {
            case 1:
                cursor->get_value(cursor, &recno, &fields[0]);
                break;
            case 2:
                cursor->get_value(cursor, &recno, &fields[0], &fields[1]);
                break;
            case 3:
                cursor->get_value(cursor, &recno, &fields[0], &fields[1], &fields[2]);
                break;
            case 4:
                cursor->get_value(cursor, &recno, &fields[0], &fields[1], &fields[2], &fields[3]);
                break;
            case 5:
                cursor->get_value(cursor, &recno, &fields[0], &fields[1], &fields[2], &fields[3], &fields[4]);
                break;
            case 6:
                cursor->get_value(cursor, &recno, &fields[0], &fields[1], &fields[2], &fields[3], &fields[4], &fields[5]);
                break;
            case 7:
                cursor->get_value(cursor, &recno, &fields[0], &fields[1], &fields[2], &fields[3], &fields[4], &fields[5], &fields[6]);
                break;
        }
        
        match = true;
        for(int i=0; i < index_nfields && match; ++i) {
            std::string &field_name = index_to_meta[wiredtiger_table].at(i);
            int field_num = table_to_field_to_num[wiredtiger_table2][field_name];
            match &= strcmp(fields[field_num], tokens.at(i+2)) == 0;
        }
        if(match == false) {
            break;
        }

        sprintf(recno_str, "%lu", recno);
        std::string buffer_write = "\n";
        buffer_write += recno_str;
        buffer_write += ">\n";
        for(int i=0; i < nfields; ++i) {
            buffer_write += table_to_meta[wiredtiger_table2].at(i);
            buffer_write += ": ";
            buffer_write += fields[i];
            buffer_write += '\n';
        }
        if(customWrite(socket_client, buffer_write.c_str(), buffer_write.length()) < 0) {
            break;
        }
    } while(cursor->next(cursor) == 0);
    
    customWrite(socket_client, "\n", 1);
    cursor->reset(cursor);
}

void remove(std::map<std::string,WT_CURSOR*> &cursor_pool, std::vector<char*> &tokens, int socket_client) {
    std::string wiredtiger_table("table:");
    wiredtiger_table += tokens.at(0);
    
    if(table_to_meta.count(wiredtiger_table) == 0) {
        std::string tmp("\nTable does not exist\n\n");
        customWrite(socket_client, tmp.c_str(), tmp.length());
        return;
    }
    
    WT_CURSOR* cursor = cursor_pool[wiredtiger_table];
    cursor->set_key(cursor, strtoul(tokens.at(1), NULL, 0));
    if(cursor->remove(cursor) == 0) {
        std::string buffer_write = "\ndeleted>\nrecno: ";
        buffer_write += tokens.at(1);
        buffer_write += "\n\n";
        customWrite(socket_client, buffer_write.c_str(), buffer_write.length());
    }
    else {
        std::string tmp("\nNot found\n\n");
        customWrite(socket_client, tmp.c_str(), tmp.length());
    }
}

void update(std::map<std::string,WT_CURSOR*> &cursor_pool, std::vector<char*> &tokens, int socket_client) {
    std::string wiredtiger_table("table:");
    wiredtiger_table += tokens.at(0);
    
    if(table_to_meta.count(wiredtiger_table) == 0) {
        std::string tmp("\nTable does not exist\n\n");
        customWrite(socket_client, tmp.c_str(), tmp.length());
        return;
    }
    
    WT_CURSOR* cursor = cursor_pool[wiredtiger_table];
    cursor->set_key(cursor, strtoul(tokens.at(1), NULL, 0));
    
    int nfields = table_to_meta[wiredtiger_table].size();
    while(tokens.size()-2 < nfields) {
        tokens.push_back(empty_string);
    }
    switch(nfields) {
        case 1:
            cursor->set_value(cursor, tokens.at(2));
            break;
        case 2:
            cursor->set_value(cursor, tokens.at(2), tokens.at(3));
            break;
        case 3:
            cursor->set_value(cursor, tokens.at(2), tokens.at(3), tokens.at(4));
            break;
        case 4:
            cursor->set_value(cursor, tokens.at(2), tokens.at(3), tokens.at(4), tokens.at(5));
            break;
        case 5:
            cursor->set_value(cursor, tokens.at(2), tokens.at(3), tokens.at(4), tokens.at(5), tokens.at(6));
            break;
        case 6:
            cursor->set_value(cursor, tokens.at(2), tokens.at(3), tokens.at(4), tokens.at(5), tokens.at(6), tokens.at(7));
            break;
        case 7:
            cursor->set_value(cursor, tokens.at(2), tokens.at(3), tokens.at(4), tokens.at(5), tokens.at(6), tokens.at(7), tokens.at(8));
            break;
    }
    
    if(cursor->update(cursor) == 0) {
        std::string buffer_write = "\nupdated>\nrecno: ";
        buffer_write += tokens.at(1);
        buffer_write += "\n\n";
        customWrite(socket_client, buffer_write.c_str(), buffer_write.length());
    }
    else {
        std::string tmp("\nNot found\n\n");
        customWrite(socket_client, tmp.c_str(), tmp.length());
    }
}

WT_CONNECTION* conn = NULL;
int port = 27000;
pthread_t thread_server;
int socket_server = -1;
std::map<pthread_t,int> thread_to_socket;

void* manageSession(void* arg) {
    WT_SESSION* session;
    conn->open_session(conn, NULL, NULL, &session);

    std::map<std::string,WT_CURSOR*> cursor_pool;
    createCursors(session, cursor_pool);

    int socket_client = thread_to_socket[pthread_self()];
    char buffer_read[999999] = {};
    int bytes_read = read(socket_client, buffer_read, 999999);

    std::vector<char*> tokens;
    parseBuffer(buffer_read, bytes_read, tokens);

    while(bytes_read > 0 && strcmp(buffer_read,"exit") != 0 && strcmp(buffer_read,"shutdown") != 0) {
        if(strcmp(buffer_read,"insert") == 0) {
            if(tokens.size() > 0) {
                insert(cursor_pool, tokens, socket_client);
            }
        }
        else if(strcmp(buffer_read,"at") == 0) {
            if(tokens.size() > 1) {
                at(cursor_pool, tokens, socket_client);
            }
        }
        else if(strcmp(buffer_read,"dump") == 0) {
            if(tokens.size() > 0) {
                dump(cursor_pool, tokens, socket_client);
            }            
        }
        else if(strcmp(buffer_read,"find") == 0) {
            if(tokens.size() > 1) {
                find(cursor_pool, tokens, socket_client);
            }
        }
        else if(strcmp(buffer_read,"delete") == 0) {
            if(tokens.size() > 1) {
                remove(cursor_pool, tokens, socket_client);
            }
        }
        else if(strcmp(buffer_read,"update") == 0) {
            if(tokens.size() > 1) {
                update(cursor_pool, tokens, socket_client);
            }
        }
        else {
            std::string tmp("\nInvalid command\n\n");
            customWrite(socket_client, tmp.c_str(), tmp.length());
        }

        buffer_read[0] = 0;
        bytes_read = 0;
        bytes_read = read(socket_client, buffer_read, 9999999);

        tokens.clear();
        parseBuffer(buffer_read, bytes_read, tokens);
    }
    
    std::cout << "disconnecting" << std::endl;
    close(socket_client);
    session->close(session, NULL);
    
    if(strcmp(buffer_read,"shutdown") == 0) {
        std::cout << "shutting down" << std::endl;
        for(auto it = thread_to_socket.begin(); it != thread_to_socket.end(); ++it) {
            if(it->first != pthread_self()) {
                close(it->second);
                pthread_cancel(it->first);
            }
        }
        pthread_cancel(thread_server);
    }
    
    return 0;
}

void* manageSockets(void* arg) {
    socket_server = socket(AF_INET, SOCK_STREAM, 0);

    int option_value = 1;
    setsockopt(socket_server, SOL_SOCKET, SO_REUSEADDR, &option_value, sizeof(option_value));
    
    sockaddr_in address_server;
    address_server.sin_family = AF_INET;
    address_server.sin_addr.s_addr = INADDR_ANY;
    address_server.sin_port = htons(port);
    
    bind(socket_server, (sockaddr*) &address_server, sizeof(address_server));
    listen(socket_server, 5);
    
    sockaddr_in address_client = {};
    int len = sizeof(address_client);
    int socket_client;
    while(true) {
        socket_client = accept(socket_server, (sockaddr*) &address_client, (socklen_t*) &len);
        if(socket_client <= 0) {
            std::cout << "error connecting" << std::endl;
            continue;
        }
        std::cout << "connected to " << inet_ntoa(address_client.sin_addr) << ':' << ntohs(address_client.sin_port) << std::endl;

        pthread_t thread_id;
        pthread_create(&thread_id, NULL, manageSession, NULL);

        thread_to_socket[thread_id] = socket_client; //possible race condition since manageSession reads this object

        address_client = {};
    }
    
    return 0;
}

void handleSignal(int signum) {
    std::cout << "Handling signal: " << signum << std::endl;
    
    for(auto it = thread_to_socket.begin(); it != thread_to_socket.end(); ++it) {
        close(it->second);
        pthread_cancel(it->first);
    }
    pthread_cancel(thread_server);
}

void parseMeta(char* cur, std::vector<std::string> &fields, std::map<std::string,int> &field_to_num) {
    while(*cur != 0) {
        if(*cur == 3) {
            return;
        }
        ++cur;
    }
    while(*cur == 0) {
        ++cur;
    }
    while(*cur != '#' && *cur != '!' && *cur != 3) {
        std::string field;
        while(*cur != 0 && *cur != '#' && *cur != '!' && *cur != 3) {
            field += *cur;
            ++cur;
        }
        fields.push_back(field);
        field_to_num[field] = fields.size()-1;
        while(*cur == 0) {
            ++cur;
        }
    }
}

int main(int argc, char* argv[]) {
    if(argc < 3) {
        std::cerr << "Usage: hellodb /path/to/meta.cfg /path/to/db/folder [port]" << std::endl;
        return -1;
    }

    std::ifstream file_stream;
    file_stream.open(argv[1]);
    if (file_stream.is_open() == false) {
        std::cerr << "failed to open meta file: " << argv[1] << std::endl;
        return -1;
    }
    char file_buffer[999999] = {};
    file_stream.read(file_buffer, 999999);
    int bytes_read = file_stream.gcount();
    file_stream.close();
    
    std::vector<char*> tokens;
    for(int i=0; i < bytes_read; ++i) {
        if(file_buffer[i] == ' ' || file_buffer[i] == 10 || file_buffer[i] == 13) {
            file_buffer[i] = 0;
        }
        else if(file_buffer[i] == '#' || file_buffer[i] == '!') {
            tokens.push_back(file_buffer+i);
        }
    }
    file_buffer[bytes_read] = 3;
  
    int ntokens = tokens.size();
    for(int i=0; i< ntokens; ++i) {
        std::vector<std::string> fields;
        std::map<std::string,int> field_to_num;
        parseMeta(tokens.at(i)+1, fields, field_to_num);
        
        std::string wiredtiger_table;
        if(*(tokens.at(i)) == '#') {
            wiredtiger_table += "table:";
            wiredtiger_table += tokens.at(i)+1;
            table_to_meta[wiredtiger_table] = std::move(fields);
            table_to_field_to_num[wiredtiger_table] = std::move(field_to_num);
        }
        else {
            wiredtiger_table += "index:";
            wiredtiger_table += tokens.at(i)+1;
            index_to_meta[wiredtiger_table] = std::move(fields);
        }
    }
    
    int ret = 0;
    if(ret == 0 && (ret = wiredtiger_open(argv[2], NULL, "create,config_base=false", &conn)) != 0) {
        std::cerr << wiredtiger_strerror(ret) << std::endl;
    }
    WT_SESSION* session = NULL;
    if(ret == 0 && (ret = conn->open_session(conn, NULL, NULL, &session)) != 0) {
        std::cerr << wiredtiger_strerror(ret) << std::endl;
    }
    if(ret == 0 && (ret = createTables(session)) != 0) {
        std::cerr << wiredtiger_strerror(ret) << std::endl;
    }
    if(ret == 0 && (ret = createIndexes(session)) != 0) {
        std::cerr << wiredtiger_strerror(ret) << std::endl;
    }
    if(session != NULL) {
        session->close(session, NULL);
    }

    if(argc > 3) {
        int tmp = strtol(argv[3], NULL, 0);
        if(tmp != 0) {
            port = tmp;
        }
    }
    
    struct sigaction sig_action = {};
    sig_action.sa_handler = handleSignal;
    sigemptyset(&sig_action.sa_mask);
    sigaddset(&sig_action.sa_mask, SIGINT); //block other signals while running handler
    sigaddset(&sig_action.sa_mask, SIGTERM); //block other signals while running handler
    
    sigaction(SIGINT, &sig_action, NULL);
    sigaction(SIGTERM, &sig_action, NULL);
    signal(SIGPIPE, SIG_IGN);
    
    if(ret == 0) {
        pthread_create(&thread_server, NULL, manageSockets, NULL);
        pthread_join(thread_server, NULL);
    }
    if(socket_server != -1) {
        close(socket_server);
    }
    if(conn != NULL) {
        conn->close(conn, NULL);
    }
    
    return ret;
}