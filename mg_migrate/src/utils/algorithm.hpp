#pragma once

#include <string>

namespace utils {

/**
 * Outputs a collection of items to the given stream, separating them with the
 * given delimiter.
 *
 * @param stream Destination stream.
 * @param first Starting iterator of collection which items are going to be
 *  printed.
 * @param last Ending iterator of the collection.
 * @param delim Delimiter that is put between items.
 * @param streamer Function which accepts a TStream and an item and streams the
 *  item to the stream.
 */
template <typename TStream, typename TIterator, typename TStreamer>
inline void PrintIterable(TStream *stream, TIterator first, TIterator last,
                          const std::string &delim = ", ",
                          TStreamer streamer = {}) {
  if (first != last) {
    streamer(*stream, *first);
    ++first;
  }
  for (; first != last; ++first) {
    *stream << delim;
    streamer(*stream, *first);
  }
}

/**
 * Outputs a collection of items to the given stream, separating them with the
 * given delimiter.
 *
 * @param stream Destination stream.
 * @param iterable An iterable collection of items.
 * @param delim Delimiter that is put between items.
 * @param streamer Function which accepts a TStream and an item and
 *  streams the item to the stream.
 */
template <typename TStream, typename TIterable, typename TStreamer>
inline void PrintIterable(TStream &stream, const TIterable &iterable,
                          const std::string &delim = ", ",
                          TStreamer streamer = {}) {
  PrintIterable(&stream, iterable.begin(), iterable.end(), delim, streamer);
}

/**
 * Outputs a collection of items to the given stream, separating them with the
 * given delimiter.
 *
 * @param stream Destination stream.
 * @param iterable An iterable collection of items.
 * @param delim Delimiter that is put between items.
 */
template <typename TStream, typename TIterable>
inline void PrintIterable(TStream &stream, const TIterable &iterable,
                          const std::string &delim = ", ") {
  PrintIterable(stream, iterable, delim,
                [](auto &stream, const auto &item) { stream << item; });
}

/**
 * Returns `true` if the given iterable contains the given element.
 *
 * @param iterable An iterable collection of values.
 * @param element The sought element.
 * @return `true` if element is contained in iterable.
 * @tparam TIiterable type of iterable.
 * @tparam TElement type of element.
 */
template <typename TIterable, typename TElement>
inline bool Contains(const TIterable &iterable, const TElement &element) {
  return std::find(iterable.begin(), iterable.end(), element) != iterable.end();
}

}  // namespace utils
