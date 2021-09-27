SRC_DIR=${PWD}/protobuf
DST_DIR=generated

clear_destination:
	rm -rf ${DST_DIR}
	mkdir -p ${DST_DIR}

build:
	@pip install grpcio-tools==1.32.0
	make clear_destination DST_DIR=${DST_DIR}
	python -m grpc_tools.protoc \
		-I=${SRC_DIR} ${SRC_DIR}/*.proto \
		--experimental_allow_proto3_optional \
		--python_out=${DST_DIR} \
	;
	# python module:
	touch ${DST_DIR}/__init__.py
	# pre-commit twice to run black again:
	pre-commit run --files generated/* || pre-commit run --files generated/* || true

update_vendored_protobuf:
	# git clone https://github.com/protocolbuffers/protobuf/tree/v3.13.0/python
	# cd protobuf/python
	# python setup.py build
	# cp -r build/lib/google/ .../_vendored/google
	# pre-commit run --files .../_vendored/google || true
