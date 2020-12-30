[[ $1 && $2 ]] || { echo "Missing the executable class name and jar"; exit 1; }

if [[ ! $SPARK_HOME ]]; then
  SPARK_HOME=/opt/spark-2.4.7
fi

class="$1"
jar="$2"
mvn clean install && $SPARK_HOME/bin/spark-submit --master local --class "$class" "$jar" 