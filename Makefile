PROJECT_DIR	:= $(shell pwd)
USER		?= $(shell id -un)
JOBS		?= $$(( $$(grep processor /proc/cpuinfo|tail -1|cut -d: -f2) + 1))
VERBOSE		?= 0

SRC_DIR		?= ${PROJECT_DIR}/src
EXTERNAL_DIR	?= ${PROJECT_DIR}/external
BSD_DIR		?= ${EXTERNAL_DIR}/bsd
INSTALL_DIR	?= ${PROJECT_DIR}/opt
BUILD_DIR	?= ${PROJECT_DIR}/build

CMAKE		?= cmake

PLATFORM = $(shell uname -n | tr a-z A-Z)
CC	:= ${INSTALL_DIR}/bin/clang

.SUFFIXES: .o .c .h

SRC_DIRS = \
	${SRC_DIR} \
	${SRC_DIR}/txm \
	${SRC_DIR}/sm \
	${SRC_DIR}/bench \
	${SRC_DIR}/lib/liblatch \
	${SRC_DIR}/lib/libmsg

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

#######################################################################
# top-level targets, checks if a call to make is required before
# calling it.
#######################################################################

.PHONY: llvm
llvm: .llvm.install_done

#######################################################################
# Install targets
#######################################################################

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
LLVM_TARGETS_TO_BUILD:= \
$$(case $$(uname -m) in \
	x86|x86_64) echo "X86;NVPTX";; \
	ppc64le) echo "PowerPC;NVPTX";; \
esac)

do-conf-llvm: .llvm.checkout_done
	[ -d ${BUILD_DIR}/llvm ] || mkdir -p ${BUILD_DIR}/llvm
	cd ${BUILD_DIR}/llvm && $(CMAKE) ${BSD_DIR}/llvm \
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

.PRECIOUS: ${BSD_DIR}/clang
.PRECIOUS: ${BSD_DIR}/compiler-rt
.PRECIOUS: ${BSD_DIR}/libcxx
.PRECIOUS: ${BSD_DIR}/libcxxabi
.PRECIOUS: ${BSD_DIR}/libunwind
.PRECIOUS: ${BSD_DIR}/llvm

do-checkout-llvm:
	# No way of adding from a top level submodules within sub-
	# modules, so stickying to this method.
	git submodule update --init --recursive ${BSD_DIR}/llvm ${BSD_DIR}/clang ${BSD_DIR}/compiler-rt ${BSD_DIR}/libcxx ${BSD_DIR}/libcxxabi ${BSD_DIR}/libunwind
	ln -sf ../../clang ${BSD_DIR}/llvm/tools/clang
	ln -sf ../../compiler-rt ${BSD_DIR}/llvm/projects/compiler-rt
	ln -sf ../../libcxx ${BSD_DIR}/llvm/projects/libcxx
	ln -sf ../../libcxxabi ${BSD_DIR}/llvm/projects/libcxxabi
	ln -sf ../../libunwind ${BSD_DIR}/llvm/projects/libunwind
	# for CUDA 9.1+ support on LLVM 6:
	#   git cherry-pick ccacb5ddbcbb10d9b3a4b7e2780875d1e5537063
	cd ${BSD_DIR}/llvm/tools/clang && git cherry-pick ccacb5ddbcbb10d9b3a4b7e2780875d1e5537063
	# for CUDA 9.2 support on LLVM 6:
	#   git cherry-pick 5f76154960a51843d2e49c9ae3481378e09e61ef
	cd ${BSD_DIR}/llvm/tools/clang && git cherry-pick 5f76154960a51843d2e49c9ae3481378e09e61ef

#######################################################################
# Makefile utils / Generic targets
#######################################################################
ifeq (${VERBOSE},0)
# Do not echo the commands before executing them.
.SILENT:
endif

.PHONY: help
help:
	@echo "-----------------------------------------------------------------------"
	@echo "The general commands are available:"
	@echo " * show-config		Display configuration variables such as paths,"
	@echo " 			number of jobs and other tunable options."
	@echo " * clean 		Remove trireme object files and binaries."
	@echo " * dist-clean		Cleans the repository to a pristine state,"
	@echo " 			just like after a new clone of the sources."
	@echo "-----------------------------------------------------------------------"
	@echo " In the following targets, '%' can be replaced by one of the external"
	@echo " project among the following list: llvm"
	@echo ""
	@echo " * clean-%		Removes the object files of '%'"
	@echo " * dist-clean-%		Removes everything from project '%', forcing a"
	@echo " 			build from scratch of '%'."
	@echo "-----------------------------------------------------------------------"

.PHONY: show-config
show-config:
	@echo "-----------------------------------------------------------------------"
	@echo "Configuration:"
	@echo "-----------------------------------------------------------------------"
	@echo "PROJECT_DIR		:= ${PROJECT_DIR}"
	@echo "SRC_DIR			:= ${SRC_DIR}"
	@echo "EXTERNAL_DIR		:= ${EXTERNAL_DIR}"
	@echo "BSD_DIR			:= ${BSD_DIR}"
	@echo "BUILD_DIR		:= ${BUILD_DIR}"
	@echo "INSTALL_DIR		:= ${INSTALL_DIR}"
	@echo "JOBS			:= ${JOBS}"
	@echo "USER			:= ${USER}"
	@echo "VERBOSE			:= ${VERBOSE}"
	@echo "-----------------------------------------------------------------------"

.PHONY: dist-clean
dist-clean:
	-rm -rf ${EXTERNAL_DIR}
	-git clean -dxf .

.PHONY: clean
clean:
	-rm -f trireme ${OBJS}

PHONY: dist-clean-%
dist-clean-%: clean-%
	-rm -rf  ${EXTERNAL_DIR}/*/$$(echo $@ | sed -e 's,dist-clean-,,')

.PHONY: clean-%
clean-%:
	-rm .$$(echo $@ | sed -e 's,clean-,,').*_done
	-rm -rf  ${BUILD_DIR}/$$(echo $@ | sed -e 's,clean-,,')

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

.PHONY: do-checkout-%
do-checkout-%:
	git submodule update --init --recursive src/$$(echo $@ | sed -e 's,do-checkout-,,')

.PRECIOUS: .%.install_done
.%.install_done: .%.build_done
	@echo "-----------------------------------------------------------------------"
	@echo "-- $$(echo $@ | sed -e 's,^[.],,' -e 's,_done,,')..."
	make do-install-$$(echo $@ | sed -e 's,^[.],,' -e 's,.install_done,,')
	@echo "-- $$(echo $@ | sed -e 's,^[.],,' -e 's,_done,,') done."
	touch $@

.PRECIOUS: .%.build_done
.%.build_done: .%.configure_done
	@echo "-----------------------------------------------------------------------"
	@echo "-- $$(echo $@ | sed -e 's,^[.],,' -e 's,_done,,')..."
	make do-build-$$(echo $@ | sed -e 's,^[.],,' -e 's,.build_done,,')
	@echo "-- $$(echo $@ | sed -e 's,^[.],,' -e 's,_done,,') done."
	touch $@

.PRECIOUS: .%.configure_done
.%.configure_done: .%.checkout_done
	@echo "-----------------------------------------------------------------------"
	@echo "-- $$(echo $@ | sed -e 's,^[.],,' -e 's,_done,,')..."
	make do-conf-$$(echo $@ | sed -e 's,^[.],,' -e 's,.configure_done,,')
	@echo "-- $$(echo $@ | sed -e 's,^[.],,' -e 's,_done,,') done."
	touch $@

.PRECIOUS: .%.checkout_done
.%.checkout_done:
	@echo "-----------------------------------------------------------------------"
	@echo "-- $$(echo $@ | sed -e 's,^[.],,' -e 's,_done,,')..."
	make do-checkout-$$(echo $@ | sed -e 's,^[.],,' -e 's,.checkout_done,,')
	@echo "-- $$(echo $@ | sed -e 's,^[.],,' -e 's,_done,,') done."
	touch $@
