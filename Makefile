# command: $make APP=appname
# 				 e.g $ make APP=gallery

ifndef APP
override APP = gallery
endif

all: client/
	rm -rf client/main
	mkdir client/main
	cp -r client/$(APP)/* client/main/
	yarn --cwd client/ install
	yarn --cwd client/ build
	rm -f log/actix.log
	RUST_BACKTRACE=full cargo run --release $(CONFIG)
	#./target/release/khameleon $(CONFIG)

test: client/
	rm -rf client/main
	mkdir client/main
	cp -r client/$(APP)/* client/main/
	yarn --cwd client/ install
	yarn --cwd client/ build
	rm -f log/actix.log
	RUST_BACKTRACE=1 cargo run