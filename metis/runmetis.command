PROG_PATH="/Users/blixhavn/Documents/metis-5.1.0/build/Darwin-x86_64/programs"
LOG_PATH="/Users/blixhavn/PycharmProjects/CollFilter/metis"

#while (( $n++ < 200 ))
for n in $(seq 93 200)
do
	$PROG_PATH/gpmetis $LOG_PATH/full.graph $n > "$n-way.out"
	echo "Done $n"
done

