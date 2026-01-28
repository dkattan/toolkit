module.exports = {
  clearMocks: true,
  moduleFileExtensions: ['js', 'ts'],
  roots: ['<rootDir>/packages'],
  testEnvironment: 'node',
  testMatch: ['**/__tests__/*.test.ts'],
  transform: {
    '^.+\\.(ts|js)$': ['ts-jest', {
      isolatedModules: true,
      diagnostics: {warnOnly: true},
      tsconfig: {
        allowJs: true,
        esModuleInterop: true
      }
    }]
  },
  transformIgnorePatterns: [
    // Some dependencies (notably Octokit packages) ship ESM and may be nested under other deps.
    // Allow transforming these packages even when they appear in nested node_modules paths.
    '/node_modules/(?!.*(@octokit|universal-user-agent|before-after-hook)/)'
  ],
  verbose: true
}
