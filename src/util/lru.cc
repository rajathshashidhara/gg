#include <lru.hh>
#include <util/path.hh>
#include <thunk/ggutils.hh>

using namespace std;

void LRU::access(const string& s)
{
  auto it = _lookup.find(s);
  if (it != _lookup.end()) {
    _list.erase(it->second);
    _current_size -= gg::hash::size(s);
  }

  _list.push_front(s);
  _lookup[s] = _list.begin();
  _current_size += gg::hash::size(s);
}

void LRU::cleanup(bool remove_file)
{
  while (_current_size > _size and _list.size() > 0)
  {
    const auto& s = _list.back();

    if (remove_file) {
      roost::path path = gg::paths::blob(s);
      roost::remove(path);
    }

    _current_size -= gg::hash::size(s);
    _lookup.erase(s);
    _list.pop_back();
  }
}
