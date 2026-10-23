The graph queries read their merges' parents and their side branches
  from caches that a chain page fills: one `parents_many` statement for
  the page's parents and one node batch for those parents, instead of
  one parents statement per merge. The walk order is unchanged.
