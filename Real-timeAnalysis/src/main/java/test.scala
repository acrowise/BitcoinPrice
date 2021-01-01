import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/**
 * @author ngt
 * @create 2020-12-20 20:19
 */

object test {
  def main(args: Array[String]): Unit = {
    //    println(new SimpleDateFormat("yyyy-MM-dd").format(1598764740*1000L))
    //
    //    println((1598764740 * 1000L) / (1000 * 60 * 60) * (1000 * 60 * 60))

    val birth = "2020-01-05 12:24";
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm")
    val date: Date = sdf.parse(birth)
    println(date)
    //    System.out.println(date); //Sun Jan 05 00:00:00 CST 2020

    println(birth.substring(0, 4))
    println(birth.substring(5, 7))
    println(birth.substring(8, 10))
    println(birth.substring(11, 13))
    println(birth.substring(14, 16))

    val timestamp: Long = 1608643878l * 1000l;
    val cal = Calendar.getInstance();
    cal.setTimeInMillis(timestamp);
    val weekDays = Array[String]("星期日", "星期一", "星期二", "星期三", "星期四", "星期五", "星期六")
    System.out.println("current day is " + cal.get(Calendar.DAY_OF_WEEK));

    println(weekDays(cal.get(Calendar.DAY_OF_WEEK)-1))

  }
}
