## Практическое задание
#### 1. Приведите пример Map only и Reduce задачи. (1 балл)
#### 2. Может ли стадия Reduce начаться до завершения стадии Map? Почему? (2 балл)
#### 3. Разверните кластер hadoop, соберите WordCount приложение, запустите на датасете ppkm_sentiment и выведите 10 самых редких слов (1 балл)
```sh
nikita@nikita-X99:~/IdeaProjects/ML_Homework_2$ sudo docker cp target/scala-2.13/ML_Homework_2-assembly-0.1.0-SNAPSHOT.jar hdp:/home/hduser
```
```sh
hduser@localhost:~$ hadoop jar ML_Homework_2-assembly-0.1.0-SNAPSHOT.jar 
hduser@localhost:~$ hdfs dfs -cat /user/hduser/ppkm_out/part-r-00000 | sort -rnk2,2 | tail 
#Beritaonline   1
#BambangIsmadi  1
#AyoJogoBlitar  1
#AniesBaswedan  1
#Anggaran"      1
#Adaptasikebiasaambaru  1
""peraturan     1
""Pilihan       1
""Kalian        1
!       1
```
#### 4. Измените маппер в WordCount так, чтобы он удалял знаки препинания и приводил все слова к единому регистру (Java: 1 балл, Scala: 2 балла)
#### 5. Перепишите WordCount на Scala (2 балла)
```sh
hduser@localhost:~$ hdfs dfs -cat /user/hduser/ppkm_out/part-r-00000 | sort -rnk2,2 | tail 
#beritahiperlokal       1
#bebascovid19   1
#banten 1
#bambangismadi  1
#ayojogoblitar  1
#atasi  1
#astagfiraullah 1
#aniesbaswedan  1
#anggaran       1
#adaptasikebiasaambaru  1
```
```scala
import ScalaWordCount.Config.{IN_PATH_DEFAULT, IN_PATH_PARAM, OUT_PATH_DEFAULT, OUT_PATH_PARAM}
import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.util.{Tool, ToolRunner}

import java.lang
import java.util.Locale
import scala.jdk.CollectionConverters.IterableHasAsScala

class MapperWordCount extends Mapper[AnyRef, Text, Text, IntWritable] {
  val keyWord = new Text
  val one = new IntWritable(1)

  override def map(key: AnyRef, value: Text,
                   context: Mapper[AnyRef, Text, Text, IntWritable]#Context): Unit =
    value.toString
      .split("\\s")
      .map(str => str.trim)
      .map(word => word.replaceAll("[.,;:\"()!?-]", ""))
      .filter(str => str.nonEmpty)
      .map(word => word.toLowerCase(Locale.ENGLISH))
      .foreach(word => {
        keyWord.set(word)
        context.write(keyWord, one)
      })
}

class ReducerWordCount extends Reducer[Text, IntWritable, Text, IntWritable] {
  val result: IntWritable = new IntWritable()

  override def reduce(key: Text, values: lang.Iterable[IntWritable],
                      context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
    val sum: Int = values.asScala
      .map(intWritable => intWritable.get())
      .sum
    result.set(sum)
    context.write(key, result)
  }

}

object ScalaWordCount extends Configured with Tool {

  override def run(args: Array[String]): Int = {
    val job: Job = Job.getInstance(getConf, "Scala word count")
    job.setJarByClass(getClass)
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    job.setMapperClass(classOf[MapperWordCount])
    job.setReducerClass(classOf[ReducerWordCount])
    job.setNumReduceTasks(1)

    val in: Path = new Path(getConf.get(IN_PATH_PARAM, IN_PATH_DEFAULT))
    val out: Path = new Path(getConf.get(OUT_PATH_PARAM, OUT_PATH_DEFAULT))
    FileInputFormat.addInputPath(job, in)
    FileOutputFormat.setOutputPath(job, out)
    val fs: FileSystem = FileSystem.get(getConf)
    if (fs.exists(out)) fs.delete(out, true)
    if (job.waitForCompletion(true)) 0 else 1
  }

  def main(args: Array[String]): Unit = {
    val exitCode = ToolRunner.run(new Configuration(), this, args)
    System.exit(exitCode)
  }

  object Config {
    val IN_PATH_PARAM: String = "scala.word.count.input"
    val IN_PATH_DEFAULT: String = "/user/hduser/ppkm"
    val OUT_PATH_PARAM: String = "scala.word.count.output"
    val OUT_PATH_DEFAULT: String = "/user/hduser/ppkm_out"
  }
}

```
#### 6. На кластере лежит датасет, в котором ключами является id сотрудника и дата, а значением размер выплаты. Руководитель поставил задачу рассчитать среднюю сумму выплат по  каждому сотруднику за последний месяц. В маппере вы отфильтровали старые записи и отдали ключ-значение вида: id-money. А в редьюсере суммировали все входящие числа и поделили результат на их  количество. Но вам в голову пришла идея оптимизировать расчет, поставив этот же редьюсер  и в качестве комбинатора, тем самым уменьшив шафл данных. Можете ли вы так сделать? Если да, то где можно было допустить ошибку? Если нет, то что должно быть на выходе  комбинатора? (2 балла)