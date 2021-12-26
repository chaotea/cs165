### This is a test dummy example of running a couple generated tests ###

#### TO BE RUN INSIDE DOCKER #####
# run from base dirctory as ./infra_scripts/test.sh

# the test directory, containing generated data dl, exp, and csvs
# expressed, relative to the base project directory
REL_TEST_DIR=generated_data
# absolute with the docker mount points
ABS_TEST_DIR=/cs165/$REL_TEST_DIR

STUDENT_OUTPUT_DIR=/cs165/student_outputs

DATA_SIZE=10000
RAND_SEED=42

# create milestone 1 data
cd project_tests/data_generation_scripts
# python milestone1.py $DATA_SIZE $RAND_SEED $ABS_TEST_DIR $ABS_TEST_DIR

# setup code
cd ../../src
rm -r data
make clean
make all

# record results of tests
# valgrind --leak-check=full --show-leak-kinds=all --track-origins=yes

echo ""
echo "-----running test 1-----"
./server > $STUDENT_OUTPUT_DIR/test01gen.server.debug.out &
sleep 1
./client < ../$REL_TEST_DIR/test01gen.dsl

echo ""
echo "-----running test 2-----"
./server > $STUDENT_OUTPUT_DIR/test02gen.server.debug.out &
sleep 1
./client < ../$REL_TEST_DIR/test02gen.dsl

echo ""
echo "-----running test 3-----"
./server > $STUDENT_OUTPUT_DIR/test03gen.server.debug.out &
sleep 1
./client < ../$REL_TEST_DIR/test03gen.dsl

echo ""
echo "-----running test 4-----"
./server > $STUDENT_OUTPUT_DIR/test04gen.server.debug.out &
sleep 1
./client < ../$REL_TEST_DIR/test04gen.dsl

echo ""
echo "-----running test 5-----"
./server > $STUDENT_OUTPUT_DIR/test05gen.server.debug.out &
sleep 1
./client < ../$REL_TEST_DIR/test05gen.dsl

echo ""
echo "-----running test 6-----"
./server > $STUDENT_OUTPUT_DIR/test06gen.server.debug.out &
sleep 1
./client < ../$REL_TEST_DIR/test06gen.dsl

echo ""
echo "-----running test 7-----"
./server > $STUDENT_OUTPUT_DIR/test07gen.server.debug.out &
sleep 1
./client < ../$REL_TEST_DIR/test07gen.dsl

echo ""
echo "-----running test 8-----"
./server > $STUDENT_OUTPUT_DIR/test08gen.server.debug.out &
sleep 1
./client < ../$REL_TEST_DIR/test08gen.dsl

echo ""
echo "-----running test 9-----"
./server > $STUDENT_OUTPUT_DIR/test09gen.server.debug.out &
sleep 1
./client < ../$REL_TEST_DIR/test09gen.dsl

echo ""
echo "-----killing server-----"
if pgrep server; then pkill server; fi
