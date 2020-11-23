#pragma once

#include <list>
#include <unordered_map>
#include <string>

class LRU {
private:
  std::list<std::string> _list {};
  std::unordered_map<std::string, std::list<std::string>::iterator> _lookup {};
  size_t _size;
  size_t _current_size { 0 };

public:
  LRU(size_t size): _size(size) {}
  void access(const std::string& s);
  void cleanup(bool remove_file = false);
  bool find(const std::string& s) const { return (_lookup.count(s) == 1 ? true : false); }
};
