# Compiler and compiler flags
CXX = g++
CXXFLAGS = -std=c++17 -O2 -Wno-deprecated-declarations

# Include paths for headers
INCLUDES = -I/opt/homebrew/include -I/usr/local/include

# Source files
SOURCES = deduplicate.cpp
OBJECTS = $(SOURCES:.cpp=.o)
EXECUTABLE = deduplicate

# Dependency installation
.PHONY: install_deps
install_deps:
	brew install rapidjson gcc

# Build the executable
$(EXECUTABLE): $(OBJECTS)
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(OBJECTS) -o $(EXECUTABLE)

# Compile the source files into object files
%.o: %.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

# Clean up object files and the executable
.PHONY: clean
clean:
	rm -f $(OBJECTS) $(EXECUTABLE)

# Rule to make everything
.PHONY: all
all: install_deps $(EXECUTABLE)

# Rule to run the program (example usage)
.PHONY: run
run: $(EXECUTABLE)
	./$(EXECUTABLE) $(FILE)