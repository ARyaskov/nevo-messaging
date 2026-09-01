import js from "@eslint/js"
import tseslint from "typescript-eslint"

export default tseslint.config(
  { ignores: ["dist/**", "node_modules/**", "examples/**", "coverage/**"] },
  js.configs.recommended,
  ...tseslint.configs.recommended,
  {
    files: ["src/**/*.ts", "src/**/*.tsx"],
    languageOptions: {
      parserOptions: {
        // The DevTools browser bundle lives under src/ but is excluded from
        // tsconfig.json (esbuild builds it), so typed linting needs its project too.
        project: ["./tsconfig.json", "./tsconfig.devtools-ui.json"],
        tsconfigRootDir: import.meta.dirname
      }
    },
    rules: {
      // Type-aware async correctness. This codebase deliberately fires background
      // loops, so an unhandled rejection is a silent failure rather than a crash.
      "@typescript-eslint/no-floating-promises": "error",
      "@typescript-eslint/no-misused-promises": ["error", { checksVoidReturn: false }],
      "@typescript-eslint/await-thenable": "error",

      "@typescript-eslint/no-explicit-any": "off",
      "@typescript-eslint/no-unused-vars": ["warn", { argsIgnorePattern: "^_", varsIgnorePattern: "^_" }],
      "@typescript-eslint/ban-ts-comment": "off",
      "@typescript-eslint/no-require-imports": "off",
      "@typescript-eslint/no-unused-expressions": ["error", { allowShortCircuit: true, allowTernary: true }],
      "no-empty": ["warn", { allowEmptyCatch: true }],
      "no-useless-escape": "off"
    }
  }
)
