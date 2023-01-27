cp testcases/testcase1/input-0.dat INPUT
cat testcases/testcase1/input-1.dat >> INPUT
cat testcases/testcase1/input-2.dat >> INPUT
cat testcases/testcase1/input-3.dat >> INPUT

cp testcases/testcase1/output-0.dat OUTPUT
cat testcases/testcase1/output-1.dat >> OUTPUT
cat testcases/testcase1/output-2.dat >> OUTPUT
cat testcases/testcase1/output-3.dat >> OUTPUT

utils/m1-arm64/bin/showsort INPUT | sort > REF_OUTPUT
utils/m1-arm64/bin/showsort OUTPUT > my_output
diff REF_OUTPUT my_output