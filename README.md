# Additional Solr streaming expression components

This repository contains a few implementations to expand the standard streaming expression
components available in Solr.

## Evaluators

- `com.damianoalbani.solr.streaming.eval.StartsWithEvaluator`  
  Wrapper around Java's [String::startsWith](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/lang/String.html#startsWith(java.lang.String)) method.

## Streams

- `com.damianoalbani.solr.streaming.stream.InnerJoinStreamOn`  
  Similar to the standard [InnerJoinStream](https://solr.apache.org/docs/8_11_1/solr-solrj/org/apache/solr/client/solrj/io/stream/InnerJoinStream.html)
  implementation, but differs from [innerJoin](https://solr.apache.org/guide/8_11/stream-decorator-reference.html#innerjoin)
  by accepting an evaluator as parameter, instead of `on` limiting to equi-joins only.

## Examples

```
innerJoinOn(
      select(
          search(
              library,
              q="type:book",
              qt="/export",
              fl="path,title",
              sort="path asc"
          ),
          path as book_path,
          title as book_title,
      ),
      select(
          search(
              library,
              q="type:chapter",
              qt="/export",
              fl="path,number,title",
              sort="path asc"
          ),
          path as chapter_path,
          number as chapter_number,
          title as chapter_title
      ),
      startsWith(getValue(_right_, val(chapter_path)), getValue(_left_, val(book_path)))
)
```

## Acknowledgments

- Special thanks to [Eric Pugh](https://github.com/epugh) for his help and inspiring example available
  at [https://github.com/epugh/playing-with-solr-streaming-expressions/](https://github.com/epugh/playing-with-solr-streaming-expressions/).
- Kudos to the whole Solr team and community for providing such a nice software!
