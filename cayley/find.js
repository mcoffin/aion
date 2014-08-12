var queries = [];
tags.forEach(function(tag, i) {
    queries[i] = g.V().Has(tag.name, tag.value)
});
var result = queries[0];
var nQueries = queries.length;
for (var i = 1; i < nQueries; i++) {
    result = result.Intersect(queries[i]);
}
result.All();
