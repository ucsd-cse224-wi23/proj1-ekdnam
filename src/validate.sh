
cp testcases/testcase2/input-0.dat INPUT
cat testcases/testcase2/input-1.dat >> INPUT
cat testcases/testcase2/input-2.dat >> INPUT
cat testcases/testcase2/input-3.dat >> INPUT

cp testcases/testcase2/output-0.dat OUTPUT
cat testcases/testcase2/output-1.dat >> OUTPUT
cat testcases/testcase2/output-2.dat >> OUTPUT
cat testcases/testcase12/output-3.dat >> OUTPUT

utils/m1-arm64/bin/showsort INPUT | sort > REF_OUTPUT
utils/m1-arm64/bin/showsort OUTPUT > my_output
diff REF_OUTPUT my_output