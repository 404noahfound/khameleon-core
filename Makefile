# command: $make APP=appname
# 				 e.g $ make APP=mosaic

ifndef APP
override APP = testapp
endif

all: client/
	rm -rf client/main
	mkdir client/main
	cp -r client/$(APP)/* client/main/
	yarn --cwd client/ install
	yarn --cwd client/ build
	rm -f log/actix.log
	cargo run --release $(CONFIG)
	#./target/release/khameleon $(CONFIG)
