cp_requirements: requirements.in
	pip-compile requirements.in --resolver=backtracking > requirements.txt
	cp requirements.txt app
	cp requirements.txt http_worker
	cp requirements.txt worker
