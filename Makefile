PROJECT_DIR	:= $(shell pwd)
USER		?= $(shell id -un)
JOBS		?= $$(( $$(grep processor /proc/cpuinfo|tail -1|cut -d: -f2) + 1))
VERBOSE		?= 0

#SRC_DIR		?= ${PROJECT_DIR}/src
INSTALL_DIR	?= ${PROJECT_DIR}/opt
BUILD_DIR	?= ${PROJECT_DIR}/build

LLVM_CHECKOUT	?= "git clone https://github.com/llvm-mirror/"
LLVM_REVISION	?= "-b release_60"

CMAKE		?= cmake

PLATFORM = $(shell uname -n | tr a-z A-Z)
CC	?= ${INSTALL_DIR}/bin/clang

.SUFFIXES: .o .c .h

SRC_DIRS = src src/txm src/sm src/bench src/lib/liblatch src/lib/libmsg

CFLAGS += -std=c99
CFLAGS += -Werror
CFLAGS += -g -ggdb
CFLAGS += -march=native -O3 -fno-omit-frame-pointer

CPPFLAGS ?=
CPPFLAGS += -D_GNU_SOURCE -D${PLATFORM}
CPPFLAGS += $(foreach dir, $(SRC_DIRS), -I$(dir)/)

LDFLAGS = -lpthread -lm -lrt -lnuma

SRCS = $(foreach dir, $(SRC_DIRS), $(wildcard $(dir)/*.c))
OBJS = $(SRCS:.c=.o)
DEPS = $(wildcard *.h)

all: llvm | trireme

trireme: ${OBJS}
	${CC} -o $@ $^ ${LDFLAGS}

%.o: %.c ${DEPS}
	${CC} -c ${CFLAGS} ${CPPFLAGS} -o $@ $<

.PHONY: clean
clean:
	-rm -f trireme ${OBJS}

#######################################################################
# top-level targets, checks if a call to make is required before
# calling it.
#######################################################################
.PHONY: llvm
llvm: .llvm.install_done

#######################################################################
# Build targets
#######################################################################
do-build-llvm: .llvm.configure_done
	cd ${BUILD_DIR}/llvm && \
		make -j ${JOBS}

#######################################################################
# Configure targets
#######################################################################
COMMON_ENV := \
 PATH=${INSTALL_DIR}/bin:${PATH} \
 CC=${INSTALL_DIR}/bin/clang \
 CXX=${INSTALL_DIR}/bin/clang++ \
 CPP=${INSTALL_DIR}/bin/clang\ -E

# LLVM_ENABLE_CXX11: Make sure everything compiles using C++11
# LLVM_ENABLE_EH: required for throwing exceptions
# LLVM_ENABLE_RTTI: required for dynamic_cast
# LLVM_REQUIRE_RTTI: required for dynamic_cast
do-conf-llvm: .llvm.checkout_done
	[ -d ${BUILD_DIR}/llvm ] || mkdir -p ${BUILD_DIR}/llvm
	cd ${BUILD_DIR}/llvm && $(CMAKE) ${PROJECT_DIR}/external/bsd/llvm \
		-DCMAKE_INSTALL_PREFIX=${INSTALL_DIR} \
		-DCMAKE_BUILD_TYPE=RelWithDebInfo \
		-DLLVM_ENABLE_CXX11=ON \
		-DLLVM_ENABLE_ASSERTIONS=ON \
		-DLLVM_ENABLE_PIC=ON \
		-DLLVM_ENABLE_EH=ON \
		-DLLVM_ENABLE_RTTI=ON \
		-DLLVM_REQUIRES_RTTI=ON \
		-DBUILD_SHARED_LIBS=ON \
		-DLLVM_USE_INTEL_JITEVENTS:BOOL=ON \
		-DLLVM_TARGETS_TO_BUILD="X86;NVPTX" \
		-Wno-dev 

#######################################################################
# Checkout sources as needed
#######################################################################

.PRECIOUS: external/bsd/llvm
external/bsd/llvm:
	eval ${LLVM_CHECKOUT}/llvm ${LLVM_REVISION} external/bsd/llvm
	eval ${LLVM_CHECKOUT}/clang ${LLVM_REVISION} external/bsd/llvm/tools/clang
	eval ${LLVM_CHECKOUT}/compiler-rt ${LLVM_REVISION} external/bsd/llvm/projects/compiler-rt
	eval ${LLVM_CHECKOUT}/libcxx ${LLVM_REVISION} external/bsd/llvm/projects/libcxx
	eval ${LLVM_CHECKOUT}/libcxxabi ${LLVM_REVISION} external/bsd/llvm/projects/libcxxabi
	eval ${LLVM_CHECKOUT}/libunwind ${LLVM_REVISION} external/bsd/llvm/projects/libunwind
	# for CUDA 9.1+ support on LLVM 6:
	#   git cherry-pick ccacb5ddbcbb10d9b3a4b7e2780875d1e5537063
	cd external/bsd/llvm/tools/clang && git cherry-pick ccacb5ddbcbb10d9b3a4b7e2780875d1e5537063
	# for CUDA 9.2 support on LLVM 6:
	#   git cherry-pick 5f76154960a51843d2e49c9ae3481378e09e61ef
	cd external/bsd/llvm/tools/clang && git cherry-pick 5f76154960a51843d2e49c9ae3481378e09e61ef

#######################################################################
# Makefile utils / Generic targets
#######################################################################
ifeq (${VERBOSE},0)
# Do not echo the commands before executing them.
.SILENT:
endif

.PHONY: show-config
show-config:
	@echo "-----------------------------------------------------------------------"
	@echo "Configuration:"
	@echo "-----------------------------------------------------------------------"
	@echo "PROJECT_DIR		:= ${PROJECT_DIR}"
#	@echo "SRC_DIR			:= ${SRC_DIR}"
	@echo "BUILD_DIR		:= ${BUILD_DIR}"
	@echo "INSTALL_DIR		:= ${INSTALL_DIR}"
	@echo "JOBS			:= ${JOBS}"
	@echo "USER			:= ${USER}"
	@echo "VERBOSE			:= ${VERBOSE}"

.PHONY: show-versions
show-versions:
	@echo "-----------------------------------------------------------------------"
	@echo "Projects:"
	@echo "-----------------------------------------------------------------------"
	@echo "LLVM_REVISION		:= ${LLVM_REVISION}"
	@echo "LLVM_CHECKOUT		:= ${LLVM_CHECKOUT}"

.PHONY: showvars
showvars:
	@make --no-print-directory show-config
	@echo
	@make --no-print-directory show-versions
	@echo "-----------------------------------------------------------------------"

%: .%.install_done

.PHONY: do-install-%
do-install-%: .%.build_done
	[ -d ${INSTALL_DIR} ] || mkdir -p ${INSTALL_DIR}
	cd ${BUILD_DIR}/$$(echo $@ | sed -e 's,do-install-,,') && \
		make -j ${JOBS} install

.PHONY: do-build-%
do-build-%: .%.configure_done
	cd ${BUILD_DIR}/$$(echo $@ | sed -e 's,do-build-,,') && \
		make -j ${JOBS}

.PHONY: do-conf-%
do-conf-%: .%.checkout_done

.PHONY: do-clean-%
do-clean-%:
	-rm .*.install_done .*.build_done .*.configure_done
	-rm -rf  ${BUILD_DIR}/$$(echo $@ | sed -e 's,do-clean-,,')

.PRECIOUS: %.install_done
%.install_done: %.build_done
	@echo "-----------------------------------------------------------------------"
	@echo "-- $$(echo $@ | sed -e 's,^[.],,' -e 's,_done,,')..."
	make do-install-$$(echo $@ | sed -e 's,^[.],,' -e 's,.install_done,,')
	@echo "-- $$(echo $@ | sed -e 's,^[.],,' -e 's,_done,,') done."
	touch $@

.PRECIOUS: %.build_done
%.build_done: %.configure_done
	@echo "-----------------------------------------------------------------------"
	@echo "-- $$(echo $@ | sed -e 's,^[.],,' -e 's,_done,,')..."
	make do-build-$$(echo $@ | sed -e 's,^[.],,' -e 's,.build_done,,')
	@echo "-- $$(echo $@ | sed -e 's,^[.],,' -e 's,_done,,') done."
	touch $@

.PRECIOUS: %.configure_done
%.configure_done: %.checkout_done
	@echo "-----------------------------------------------------------------------"
	@echo "-- $$(echo $@ | sed -e 's,^[.],,' -e 's,_done,,')..."
	make do-conf-$$(echo $@ | sed -e 's,^[.],,' -e 's,.configure_done,,')
	@echo "-- $$(echo $@ | sed -e 's,^[.],,' -e 's,_done,,') done."
	touch $@

.PRECIOUS: %.checkout_done
.%.checkout_done: external/bsd/%
	@echo "-----------------------------------------------------------------------"
	@echo "-- $$(echo $@ | sed -e 's,^[.],,' -e 's,_done,,') done."
	touch $@
