A packet for a hired reviewer: [docs/security-review.md](docs/
  security-review.md) states the review's scope and its out-of-scope
  boundaries, links the trust model instead of copying it, and lists
  the internal attack passes by surface — what each tried, what it
  found, and the pull request that closed it — so a reviewer starts
  past them. It walks a ten-minute local build-and-attack setup, links
  the fuzz target map, and names where the project itself wants the
  most eyes, strongest first. No exploit recipes: every named weakness
  is closed with a test or pinned as documented behavior.
