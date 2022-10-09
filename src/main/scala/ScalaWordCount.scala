import ScalaWordCount.Config.{IN_PATH_PARAM, IN_PATH_DEFAULT, OUT_PATH_PARAM, OUT_PATH_DEFAULT}
import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.util.{Tool, ToolRunner}

import java.lang
import scala.jdk.CollectionConverters.IterableHasAsScala

class MapperWordCount extends Mapper[AnyRef, Text, Text, IntWritable] {
  val keyWord = new Text
  val one = new IntWritable(1)

  override def map(key: scala.AnyRef, value: Text, context: Mapper[AnyRef, Text, Text, IntWritable]#Context): Unit =
    value.toString
      .split("\\s")
      .map(str => str.replaceAll("[.,;:\"()!?-]", ""))
      .map(str => str.toLowerCase)
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
    job.setOutputKeyClass(classOf[IntWritable])
    job.setOutputValueClass(classOf[Text])
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
