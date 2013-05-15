COVERAGE_FILES=`find txzookeeper -name "*py" | grep -v "tests"`

coverage:
	python -c "import coverage as c; c.main()" run /usr/bin/trial txzookeeper
	python -c "import coverage as c; c.main()" html -d htmlcov $(COVERAGE_FILES)
	gnome-open htmlcov/index.html

default:
	echo "done"


.DEFAULT_GOAL := default
