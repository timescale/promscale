# Release checklist

## Step-by-step release process

### Prepare release PR
  - [ ] Create a branch named `release-{version}`:
    - [ ] If it's a patch release, based the new branch out of the one that
      contains the fixes. For example, if we added some fixes to the already
      released `release-0.12` branch, then it will be the base for our `0.12.1`
      release.
  - [ ] Update Promscale in `pkg/version/version.go` update. If required, update
    the values for PrevReleaseVersion, PgVersionNumRange and
    ExtVersionRangeString.
    - [ ] Update `EXTENSION_VERSION` if there is a change in ExtVersionRangeString
  - [ ] If the release includes changes to prom-migrator, update the `version`
    variable at `migration-tool/cmd/prom-migrator/main.go`.
  - [ ] (optionally) Update `prom-migrator` version in `.goreleaser.yml` (2 places)
  - [ ] Finalize CHANGELOG, adding release version and date
  - [ ] Commit: `git commit -a -m "Prepare for the 0.1.0-alpha.4 release"`
  - Create PR:
    - [ ] For major/minor releases the PR should target the master branch.
    - [ ] For patch releases the PR should target the release branch it was
      based of.
  - Wait for review and merge.

### Prepare git tag
  - [ ] Pull the branch were the PR was merged, it should be `master` for
    major/minor and `release-{version}` for patch releases. Triple check that
    you have the right commit checked out.
  - [ ] tag commit: `git tag -a 0.1.0-alpha.1 -m "Release 0.1.0-alpha.1"`
  - [ ] push tag to main repo (not in fork!) `git push origin 0.1.0-alpha.1`

### Create GitHub release notes

  - [ ] The `goreleaser` GitHub Action will automatically create a new draft release with the generated binaries, docker images, and a changelog attached. When it is created contact PM to validate if release notes are correct and click green publish button.

### Post-release

For major/minor releases:

 - [ ] Update Promscale and PrevReleaseVersion in `pkg/version/version.go` to the
   next devel version.
 - [ ] If there is a hard dependency on promscale_extension version, make sure to modify [pkg/tests/upgrade_tests/upgrade_test.go#getDBImages](https://github.com/timescale/promscale/blob/master/pkg/tests/upgrade_tests/upgrade_test.go#L89-L92) as done in this [commit](https://github.com/timescale/promscale/pull/1516/commits/6e2434d51dfd3e91505049a2828add3266f3e0f8#diff-6343d0a8cf4936b8f948769738eef8b0624d15d13ccc0a53b457e4f5c53b14e6R90-R94) to return timescale image which has the required promscale_extension. Missing to do so will cause `TestUpgradeFromPrev` failure.
 - [ ] Commit: `git commit -a -m "Prepare for the next development cycle"`
 - [ ] Create PR & Merge when ready

For patch releases:

 - [ ] Update Promscale docs to point to the latest release as done in this
   [PR](https://github.com/timescale/docs/pull/1075).
 - [ ] Create a branch based on master.
 - [ ] Cherry-pick the commits related to the patch.
 - [ ] Merge back into master.

Take a breath. You're done releasing.

## Release notes guide

Here are a couple of good examples of release notes which can be used for comparison:
- [Full release 0.6.0](https://github.com/timescale/promscale/releases/tag/0.6.0)
- [Patch release 0.6.2](https://github.com/timescale/promscale/releases/tag/0.6.2)

The following sections describe the different aspects of the release which should be highlighted.

### Headline Features

Describe the major features of this release. Point to announcements or blog posts for reference.

### Thanks

It's nice to shout out to community members who've contributed some changes. Use the following to find all authors (and then figure out who are internal and external on your own):

```
git shortlog --summary --numbered --email <PREV_RELEASE>..<CURRENT_RELEASE>
```

### Deprecations

Highlight functionality which has been deprecated in this release, and give and indication of when users can expect that the deprecated functionality will be removed.

### Backwards-incompatible changes

Highlight functionality which has been removed in this release, and point out the alternatives (if present).

### Upgrade notes

Provide any notes on special steps that users must take during the upgrade, e.g. configuration changes which must (or can) be made.

### Changelog

This will be automatically populated by `goreleaser`. Remove all bors merge commits and dependabot version bump commits.
