export default {
  branches: [{ name: "main" }],
  plugins: [
    "@semantic-release/commit-analyzer",
    "@semantic-release/release-notes-generator",
    [
      "@semantic-release/changelog",
      {
        changelogFile: "CHANGELOG.md",
      },
    ],
    [
      "@semantic-release/exec",
      {
        prepareCmd:
          "sh -c '" +
          "if sed --version >/dev/null 2>&1; then " +
          "sed -i \"s/^version[[:space:]]*=[[:space:]]*\\\".*\\\"/version = \\\"${nextRelease.version}\\\"/\" pyproject.toml; " +
          "else " +
          "sed -i \"\" \"s/^version[[:space:]]*=[[:space:]]*\\\".*\\\"/version = \\\"${nextRelease.version}\\\"/\" pyproject.toml; " +
          "fi'",
      },
    ],
    [
      "@semantic-release/git",
      {
        assets: ["CHANGELOG.md", "pyproject.toml"],
        message: "chore(release): ${nextRelease.version} [skip ci]"
      }
    ],
  ],
};