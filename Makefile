
binary = dirsplit
sources = dirsplit.cpp
objects = $(sources:.cpp=.o)

build ?= debug
xflags := /$(build)
xflags := $(xflags:/debug=-O0 -D_DEBUG)
xflags := $(xflags:/release=-O2 -DNDEBUG)

CXXFLAGS += -std=c++11 -Wall -Wextra -g $(xflags)
LDFLAGS += -g

all: $(binary)

clean:
	rm -f $(binary) $(objects)

$(binary): $(objects)
	$(CXX) $(LDFLAGS) -o $(binary) $(objects)
