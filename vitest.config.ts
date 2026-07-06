import { defineConfig } from "vitest/config";

/**
 * vitest.config.ts — gg-observability test runner.
 *
 * `pool: "forks"` runs each test file in its own Node process. This avoids
 * chokidar's fsevents stream being shared across vitest worker_threads, which
 * caused T6.a (chokidar integration) to flake under the full suite.
 *
 * `fileParallelism: false` then serializes the files, ensuring T6.a gets a
 * clean event loop and an exclusive fsevents watcher.
 */
export default defineConfig({
	test: {
		pool: "forks",
		fileParallelism: false,
		testTimeout: 30_000,
		hookTimeout: 30_000,
	},
});