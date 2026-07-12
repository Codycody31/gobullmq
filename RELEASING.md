# Releasing

This repository maps one Go minor release line to one exact BullMQ version.
The branch name, compatibility manifest, Node lockfile, vendored Lua scripts,
and release notes must agree before a tag can be created.

## Release branches

Name release branches using both version axes:

```text
release/v<go-major>.<go-minor>-bullmq-v<bullmq-major>.<bullmq-minor>.<bullmq-patch>
```

The first line is `release/v1.1-bullmq-v4.12.2`. Its releases are `v1.1.0`,
`v1.1.1`, and later patch versions. Changing the BullMQ baseline requires a
new release branch and Go minor version. A breaking Go API or wire change
requires a new Go major version and matching module-path suffix.

Create a release branch only from a `main` commit where `CI / required` is
green. Update `compatibility/bullmq.json` first, run `make verify-compat`, then
push the branch and confirm that the branch name matches the manifest. The
branch keeps that BullMQ tag and commit for its entire lifetime.

## Creating a release

1. Merge changes through a squash PR whose title follows Conventional Commits.
2. Wait for `CI / required` to pass on the release branch.
3. Release Please creates or refreshes a draft release PR.
4. Start CI for the draft PR manually. The built-in `GITHUB_TOKEN` does not
   trigger another Actions run for the PR it creates. Reopening the draft as a
   maintainer triggers the normal PR workflow; approve the run if GitHub queues
   it for approval.
5. Review the version, changelog, compatibility details, and installation
   command.
6. Mark the release PR ready and merge it only when the version should become
   public.

Merging the release PR creates the Git tag and GitHub Release. That tag is the
Go module publication event and cannot be moved or reused. The release workflow
then verifies direct download, proxy indexing, and compilation from an external
consumer module.

Never create, delete, or retarget a release tag manually. If a published
version is defective, make the fix on its release branch, add a `retract`
directive when appropriate, and publish a higher patch.

## Backports

Land a shared fix on the newest applicable line first. Cherry-pick it onto each
maintained release branch using the same `fix:` title, resolve differences in a
PR, and let Release Please produce an independent patch release. A fix that
only applies to an older line may target that release branch directly.

Feature and breaking-change PRs are rejected on fixed release branches.

## Commit and PR text

Use Conventional Commit titles such as `fix(worker): renew locks while
draining`, `feat(queue): add a getter`, or `feat!: change the worker contract`.
Squash merges use the PR title as the commit title. Do not add co-author
trailers or generated-with attribution to commits or PRs.
