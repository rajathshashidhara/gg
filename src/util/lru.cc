#include <lru.hh>
#include <util/path.hh>
#include <thunk/ggutils.hh>

using namespace std;

void LRU::access(const string& s, bool pin)
{
  auto it = _lookup.find(s);
  if (it != _lookup.end()) {
    _list.erase(it->second);
    _current_size -= gg::hash::size(s);
  }

  _list.push_back(s);
  list<string>::iterator i = _list.end();
  i--;  /* get iterator to last element */
  _lookup[s] = i;

  _ref_cnt[s] += (pin == true ? 1 : 0);
  _current_size += gg::hash::size(s);
}

void LRU::cleanup(bool remove_file)
{

  for (auto it = _list.begin(); it != _list.end();)
  {
    if (_current_size <= _size)
      return;

    if (_ref_cnt[*it] > 0) {
      ++it;
      continue;
    }

    if (remove_file) {
      roost::path path = gg::paths::blob(*it);
      roost::remove(path);
    }

    _current_size -= gg::hash::size(*it);
    _ref_cnt.erase(*it);
    _lookup.erase(*it);
    it = _list.erase(it);
  }

}

void LRU::unpin(const string& s)
{
  assert(_ref_cnt[s] > 0);
  _ref_cnt[s] -= 1;
}
