docker build -t check_pep8 -f ".github/workflows/check_pep8/Dockerfile" .
docker run -v .:/app check_pep8