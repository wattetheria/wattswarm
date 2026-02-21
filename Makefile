.PHONY: test test-report test-junit test-coverage

test:
	cargo test --workspace

test-junit:
	cargo nextest run --workspace --junit-file target/test-report/junit.xml

test-coverage:
	cargo llvm-cov --workspace --lcov --output-path target/test-report/lcov.info
	cargo llvm-cov --workspace --html --output-dir target/test-report/html

test-report:
	./scripts/test-report.sh
