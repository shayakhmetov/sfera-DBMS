all:
	gcc db.c node_alloc.c work_with_disk.c work_with_key.c insert.c search.c delete.c cache.c -shared -fPIC -o db.so -O3 -D __CACHE__
without_cache:
	gcc db.c node_alloc.c work_with_disk.c work_with_key.c insert.c search.c delete.c cache.c -shared -fPIC -o db.so -O3 
debug:
	gcc db.c node_alloc.c work_with_disk.c work_with_key.c insert.c search.c delete.c print_for_debug.c cache.c -shared -fPIC -o db.so -ggdb -O0
noshared:
	gcc main.c db.c node_alloc.c work_with_disk.c work_with_key.c insert.c search.c delete.c print_for_debug.c cache.c -o db.out -ggdb -O0
clean:
	rm -f db.so db.out temp.txt result.txt
