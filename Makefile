setup:
	git config core.hooksPath .githooks
	chmod -R +x .githooks/
	@echo "Git hooks configured!"
