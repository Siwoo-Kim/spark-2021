[[ $1 && $2 ]] || exit 1

if [[ ! $SPARK_HOME ]]; then
  SPARK_HOME=/opt/spark-2.4.7
fi

class="$1"
jar="$2"
mvn clean install && $SPARK_HOME/bin/spark-submit --master local --class "$class" "$jar" 