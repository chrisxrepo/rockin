#rockin Makefile

CC     := gcc
CXX    := g++
LD     := g++
AR     := ar rc
RANLIB := ranlib
MAKE	 := make

OBJ_DIR	:= .objs

EXTRA_CFLAGS  :=
EXTRA_LDFLAGS :=


ifeq ($(PLATFORM), OS_AIX)
SNAPPY_MAKE_TARGET = libsnappy.la
endif

all: tmp_dir $(DEP_LIB) rockin

tmp_dir:
	mkdir -p $(OBJ_DIR)

CXXFLAGS := -std=c++11 -O2 -fPIC
LINKFLAGS := -lrocksdb -luv -lglog -lgflags -lpthread -ljemalloc 
INCLUDE :=  -I include 

SRCS := $(wildcard *.cc src/*.cc) 
OBJS := $(patsubst src/%.cc, $(OBJ_DIR)/%.o,$(SRCS))

rockin: $(OBJS)
	$(CXX) $(CFLAGS) -o $@ $^ $(CXXFLAGS) $(LINKFLAGS)

$(OBJ_DIR)/%.o : src/%.cc
	${CXX} -c ${CXXFLAGS} $(INCLUDE) $< -o $@

%.o : %.c
	${CC} -c ${CFLAGS} $(INCLUDE)  $< -o $@

clean:
	rm -fr rockin
	rm -fr $(LIB_DIR)
	rm -fr $(OBJ_DIR)
