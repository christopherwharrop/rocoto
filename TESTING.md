# Testing Infrastructure Summary

## Current Status

✅ **RSpec infrastructure:** Fully operational  
✅ **All tests passing:** 101/101 examples (100%)  
✅ **Code coverage:** 4.82% (442/9162 lines tracked)

Coverage includes all 52 Ruby files in `lib/`. The current tests cover 8 files (cycledef, workflowoption,
workflowdb, and dependencies). Coverage will increase as you refactor and add tests for currently untested
modules.

## Running Tests

### Locally (after installation with Ruby 3.2+)

```bash
# Run all tests
bundle exec rake spec

# Run with coverage HTML report (generates coverage/index.html)
bundle exec rake coverage

# Run with coverage shown in terminal
bundle exec rake coverage_terminal

# Run specific spec
bundle exec rspec spec/workflowmgr/cycledef_spec.rb

# Run specs matching pattern
bundle exec rspec -e "exclude_hours"
```

### In CI
Tests run automatically on every push/PR via GitHub Actions with Ruby 3.2.0, 3.2.x, 3.3.0, and 3.3.x.

## References

* [RSpec Documentation](https://rspec.info/)
* [RSpec Best Practices](https://www.betterspecs.org/)
* [SimpleCov](https://github.com/simplecov-ruby/simplecov)

## Linting and Style Checks (RuboCop)

RuboCop is used to enforce Ruby style and catch common issues. Run it before submitting changes:

```bash
# Lint the codebase
bundle exec rubocop

# Auto-correct safe issues
bundle exec rubocop -a

# Auto-correct all (safe and unsafe) issues
bundle exec rubocop -A
```

Check `.rubocop.yml` for project-specific rules. Review all auto-corrected changes before committing.
