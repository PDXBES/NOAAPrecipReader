using System;
using System.Collections.Generic;
using System.Linq;
using System.Collections;
using System.Text;
using System.IO;

namespace NoaaPrecipReader
{
  class Program
  {
    private static double SCALING_FACTOR = 1.0732;
    private static Dictionary<DateTime, double> allPPT = new Dictionary<DateTime, double>();
    private static Dictionary<DateTime, double> allET = new Dictionary<DateTime, double>();
    static void Main(string[] args)
    {
      //ReadRawNoaaPrecip();
      ProcessPPT();
      //ProcessET();
      WriteSummary();
    }

    private static void WriteSummary()
    {
      TextWriter writer = new StreamWriter(@"\\cassio\storm_sysplan\Models\Stephens\Analysis\TimeSeries\Precip\PDXCollins15MinExport_4sf_summary.txt");

      IEnumerable<IGrouping<int, double>> query = allPPT.GroupBy(p => p.Key.Year, p => p.Value);

      writer.WriteLine("Precip summary:");
      foreach (IGrouping<int, double> group in query)
      {
        writer.WriteLine(group.Key + "," + group.Sum().ToString("0.0"));
      }

      writer.Close();
    }

    private static void ProcessPPT()
    {
      TextReader reader = new StreamReader(@"\\cassio\storm_sysplan\Models\Stephens\Analysis\TimeSeries\Precip\PDXCollins15MinExport_4sf.csv");
      TextWriter writer = new StreamWriter(@"\\cassio\storm_sysplan\Models\Stephens\Analysis\TimeSeries\Precip\StephensWDMInputs_PPT_processed.csv");

      string header = reader.ReadLine();
      reader.ReadLine(); //dummy line

      writer.WriteLine("Date,Time,Precip,Source,ScalingFactor");

      string line = reader.ReadLine();

      while (line != null)
      {
        string[] tokens = line.Split(new char[] { '\t', ',' }, StringSplitOptions.None);
        string metadata;

        DateTime dt = DateTime.Parse(tokens[0] + " " + tokens[1]);
        double precip;
        if (!Double.TryParse(tokens[2], out precip))
        {
          if (!Double.TryParse(tokens[3], out precip))
          {
            precip = 0;
            metadata = "MISSING,null";
          }
          else
          {
            precip *= SCALING_FACTOR;
            metadata = "NOAA PDX," + SCALING_FACTOR;
          }
        }
        else
        {
          metadata = "HYDRA Collins View,1.0";
        }

        allPPT.Add(dt, precip);
        line = reader.ReadLine();
        writer.WriteLine(dt.ToString("MM/dd/yyyy,HH:mm:ss") + "," + precip.ToString("0.000") + "," + metadata);
      }

      reader.Close();
      writer.Close();

    }

    private static void ProcessET()
    {
      TextReader reader = new StreamReader(@"\\cassio\storm_sysplan\Models\Stephens\Analysis\TimeSeries\StephensWDMInputs_ET_PPT.txt");
      TextWriter writer = new StreamWriter(@"\\cassio\storm_sysplan\Models\Stephens\Analysis\TimeSeries\StephensWDMInputs_ET_processed.csv");

      string header = reader.ReadLine();
      reader.ReadLine(); //dummy line

      writer.WriteLine("DateTime,ET,Source");

      string line = reader.ReadLine();

      while (line != null)
      {
        string[] tokens = line.Split(new char[] { '\t' }, StringSplitOptions.None);
        string metadata;

        DateTime dt = DateTime.Parse(tokens[0] + " " + tokens[1]);
        double et;
        if (!Double.TryParse(tokens[4], out et))
        {
          if (!Double.TryParse(tokens[5], out et))
          {
            et = 0;
            metadata = "MISSING,null";
          }
          else
          {
            metadata = "Average";
          }
        }
        else
        {
          metadata = "PDX";
        }

        allET.Add(dt, et);
        line = reader.ReadLine();
        writer.WriteLine(dt.ToString("MM/dd/yyyy,hh:mm:ss") + "," + et.ToString("0.0000") + "," + metadata);
      }

      reader.Close();
      writer.Close();

    }

    private static void ReadRawNoaaPrecip()
    {
      TextReader reader = new StreamReader(@"\\cassio\storm_sysplan\Models\Stephens\Analysis,imeSeries\Precip\NOAA PDX Hourly Precip 1948-2010.TXT");
      TextWriter writer = new StreamWriter(@"\\cassio\storm_sysplan\Models\Stephens\Analysis,imeSeries\Precip\NOAA_PDX_Hourly_processed.csv");

      string header = reader.ReadLine();
      reader.ReadLine(); //dummy line

      string line = reader.ReadLine();

      SortedDictionary<DateTime, string> flows = new SortedDictionary<DateTime, string>();

      bool inDownTime = false;

      while (line != null)
      {
        string[] tokens = line.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
        int year = int.Parse(tokens[4]);
        int month = int.Parse(tokens[5]);
        int day = int.Parse(tokens[6]);

        DateTime dt = new DateTime(year, month, day);

        for (int i = 7; i < tokens.Length - 4; i += 4)
        {
          string flag = tokens[i + 2];
          double rain = double.Parse(tokens[i + 1]);

          if (flag == "[")
          {
            flag = "missing";
            rain = 99999;
            inDownTime = true;
          }
          else if (flag == "]")
          {
            flag = "missing";
            rain = 99999;
            inDownTime = false;
          }

          if (inDownTime)
          {
            flag = "missing";
            rain = 99999;
          }
          else if (rain != 99999)
            rain = double.Parse(tokens[i + 1]) / 100;


          flows.Add(dt, rain + "," + flag);
          dt += new TimeSpan(1, 0, 0);
        }
        line = reader.ReadLine();
      }

      SortedDictionary<DateTime, string> filledFlows = new SortedDictionary<DateTime, string>();
      DateTime min = flows.Keys.Min();
      DateTime max = flows.Keys.Max();
      for (DateTime dt = min; dt <= max; dt += new TimeSpan(1, 0, 0))
      {
        filledFlows.Add(dt, string.Empty);
      }

      foreach (DateTime dt in filledFlows.Keys)
      {
        writer.WriteLine(dt.ToString("M/d/yyyy H:mm:ss") + "," + (flows.ContainsKey(dt) ? flows[dt] : "0,filled"));
      }

      reader.Close();
      writer.Close();

      return;
    }
  }
}
