# Setup note: adding spark-mllib (read before running Phases 16-18)

Your `build.sbt` wasn't included in the zip you uploaded (it's not
gitignored - `data/`, `target/`, IDE folders are - so it likely just
wasn't part of what got zipped from your machine). Phases 14-15 only need
`spark-core`/`spark-sql`, already required since Phase 5, but Phases
16-18 additionally need `spark-mllib`.

Open your `build.sbt` and confirm it has a line like:

```scala
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.5.1"
```

Use the **exact same version** as your existing `spark-core`/`spark-sql`
lines (your terminal output confirmed `Running Spark version 3.5.1`, so
that's almost certainly already `3.5.1` in your file - just match it,
don't introduce a second Spark version). If your existing dependencies
use `Seq(...)` or `++=` rather than individual `+=` lines, add it in
whichever style the rest of the file already uses, for consistency.

After adding it:

```bash
cd ~/startup-survival-prediction/scala
sbt clean compile
```

If this fails with a dependency resolution error rather than a code
error, that confirms the version string doesn't match what's actually
available - check your existing `spark-core` line's exact version string
and copy it precisely.
