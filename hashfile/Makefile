hp:
	@echo " Compile ht_main ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/ht_main.c ./src/hash_file.c -lbf -o ./build/runner -O2
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/ht_main1.c ./src/hash_file.c -lbf -o ./build/runner1 -O2
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/ht_main2.c ./src/hash_file.c -lbf -o ./build/runner2 -O2
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/ht_main3.c ./src/hash_file.c -lbf -o ./build/runner3 -O2

bf:
	@echo " Compile bf_main ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/bf_main.c -lbf -o ./build/runner -O2

