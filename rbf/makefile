include ../makefile.inc

all: librbf.a rbftests

# c file dependencies
pfm.o: pfm.h $(CODEROOT)/util/errcodes.h
rbfm.o: rbfm.h $(CODEROOT)/util/errcodes.h
errcodes.o: $(CODEROOT)/util/errcodes.h

# lib file dependencies
librbf.a: librbf.a(pfm.o)  # and possibly other .o files
librbf.a: librbf.a(rbfm.o)
librbf.a: librbf.a($(CODEROOT)/util/errcodes.o)

rbftests.o: pfm.h rbfm.h $(CODEROOT)/util/errcodes.h

# binary dependencies
rbftests:  rbftests.o  librbf.a $(CODEROOT)/rbf/librbf.a

# dependencies to compile used libraries
.PHONY: $(CODEROOT)/rbf/librbf.a
$(CODEROOT)/rbf/librbf.a:
	$(MAKE) -C $(CODEROOT)/rbf librbf.a

.PHONY: clean
clean:
	-rm rbftest *.a *.o *~
