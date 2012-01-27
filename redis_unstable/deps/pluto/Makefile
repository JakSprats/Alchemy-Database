TARGETS=$(shell ls makefile.* | sed -e "s/makefile.//")

none:
	@echo "Please specify a target platform: $(TARGETS)"

$(TARGETS):
	@make -f makefile.$@

clean:
	rm -f *.o *.so *.dylib *.lo *.la *.dll *.exe pptest puptest

# how to get this to depend on linux/macosx/mingw
test:
	./pptest
	./puptest
