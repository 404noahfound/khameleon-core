log_name=$1
filename="log/${log_name}_run_time.log"
make | grep "scheduler time:" > $filename
sed -i '$d' $filename