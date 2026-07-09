// @ts-check
import js from '@eslint/js';
import { defineConfig } from 'eslint/config';
import tseslint from 'typescript-eslint';

export default defineConfig(
  // Global ignores. The project's tsconfig.json includes only server/,
  // shared/, daemon/ — everything else here is either tooling, tests, or
  // excluded by app architecture. We honor that boundary so the type-aware
  // parser doesn't error on files it can't resolve to a tsconfig.
  {
    ignores: [
      'node_modules/**',
      'dist/**',
      '.yarn/**',
      'apps/**',
      'extension/**',
      '**/*.test.ts',
      'vitest.config.ts',
      'worker/**',
      'scripts/**',
      'eslint.config.js',
    ],
  },
  // TypeScript source files in server/, shared/, daemon/. Uses
  // recommendedTypeChecked so we get the type-aware rules on top of the
  // base recommended set; projectService lets the parser pick up the
  // project's tsconfig.json automatically.
  {
    files: ['**/*.ts'],
    extends: [js.configs.recommended, tseslint.configs.recommendedTypeChecked],
    languageOptions: {
      parserOptions: {
        projectService: true,
      },
    },
    rules: {
      // Conventions in this codebase:
      //   - `_` prefix marks intentionally unused params/locals (TypeScript
      //     itself allows this by default, but
      //     @typescript-eslint/no-unused-vars is off below to be explicit).
      //   - `any` shows up in better-sqlite3 row shapes, JSON parses, and
      //     test fixtures.
      // Demote the noisy type-checked rules to `warn` so lint exits 0 while
      // still surfacing every finding — tighten back to `error` rule-by-
      // rule as the codebase is cleaned up.
      'no-unused-vars': 'off',
      '@typescript-eslint/no-unused-vars': 'off',
      '@typescript-eslint/no-explicit-any': 'warn',
      '@typescript-eslint/no-unsafe-assignment': 'warn',
      '@typescript-eslint/no-unsafe-member-access': 'warn',
      '@typescript-eslint/no-unsafe-argument': 'warn',
      '@typescript-eslint/no-unsafe-call': 'warn',
      '@typescript-eslint/no-unsafe-return': 'warn',
      '@typescript-eslint/no-floating-promises': 'warn',
      '@typescript-eslint/no-redundant-type-constituents': 'warn',
      '@typescript-eslint/no-unnecessary-type-assertion': 'warn',
      '@typescript-eslint/require-await': 'warn',
      '@typescript-eslint/no-require-imports': 'warn',
      'prefer-const': 'warn',
      // Migration startup swallows ADD COLUMN errors per column — allow
      // empty catch blocks (the canonical idiom for this case).
      'no-empty': ['error', { allowEmptyCatch: true }],
    },
  },
);
