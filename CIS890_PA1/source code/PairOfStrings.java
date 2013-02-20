// from cloud


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UTFDataFormatException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class PairOfStrings
  implements WritableComparable<PairOfStrings>
{
  private String leftElement;
  private String rightElement;

  public PairOfStrings()
  {
  }

  public PairOfStrings(String left, String right)
  {
    set(left, right);
  }

  public void readFields(DataInput in)
    throws IOException
  {
    this.leftElement = in.readUTF();
    this.rightElement = in.readUTF();
  }

  public void write(DataOutput out)
    throws IOException
  {
    out.writeUTF(this.leftElement);
    out.writeUTF(this.rightElement);
  }

  public String getLeftElement()
  {
    return this.leftElement;
  }

  public String getRightElement()
  {
    return this.rightElement;
  }

  public void set(String left, String right)
  {
    this.leftElement = left;
    this.rightElement = right;
  }

  public boolean equals(Object obj)
  {
    PairOfStrings pair = (PairOfStrings)obj;
    return (this.leftElement.equals(pair.getLeftElement())) && (this.rightElement.equals(pair.getRightElement()));
  }

  public int compareTo(PairOfStrings obj)
  {
	  PairOfStrings pair = obj;

	    String pl = pair.getLeftElement();
	    String pr = pair.getRightElement();
/*
	    if (this.leftElement.equals(pl)) {
	      return this.rightElement.compareTo(pr);
	    }
*/
	    return this.leftElement.compareTo(pl);
   }

  public int hashCode()
  {
    return this.leftElement.hashCode() + this.rightElement.hashCode();
  }

  public String toString()
  {
    return "(" + this.leftElement + ", " + this.rightElement + ")";
  }

  public PairOfStrings clone()
  {
    return new PairOfStrings(this.leftElement, this.rightElement);
  }

  static
  {
    WritableComparator.define(PairOfStrings.class, new Comparator());
  }

  public static class Comparator extends WritableComparator
  {
    public Comparator()
    {
      super(null);
    }

    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
    {
      String thisLeftValue = readUTF(b1, s1);
      String thatLeftValue = readUTF(b2, s2);

      if (thisLeftValue.equals(thatLeftValue)) {
        int s1offset = readUnsignedShort(b1, s1);
        int s2offset = readUnsignedShort(b2, s2);

        String thisRightValue = readUTF(b1, s1 + 2 + s1offset);
        String thatRightValue = readUTF(b2, s2 + 2 + s2offset);

        return thisRightValue.compareTo(thatRightValue);
      }

      return thisLeftValue.compareTo(thatLeftValue);
    }

    private String readUTF(byte[] bytes, int s)
    {
      try
      {
        int utflen = readUnsignedShort(bytes, s);

        byte[] bytearr = new byte[utflen];
        char[] chararr = new char[utflen];

        int count = 0;
        int chararr_count = 0;

        System.arraycopy(bytes, s + 2, bytearr, 0, utflen);

        while (count < utflen) {
          int c = bytearr[count] & 0xFF;
          if (c > 127)
            break;
          count++;
          chararr[(chararr_count++)] = (char)c;
        }

        while (count < utflen) {
          int c = bytearr[count] & 0xFF;
          int char2;
          switch (c >> 4)
          {
          case 0:
          case 1:
          case 2:
          case 3:
          case 4:
          case 5:
          case 6:
          case 7:
            count++;
            chararr[(chararr_count++)] = (char)c;
            break;
          case 12:
          case 13:
            count += 2;
            if (count > utflen) {
              throw new UTFDataFormatException("malformed input: partial character at end");
            }
            char2 = bytearr[(count - 1)];
            if ((char2 & 0xC0) != 128)
              throw new UTFDataFormatException("malformed input around byte " + count);
            chararr[(chararr_count++)] = (char)((c & 0x1F) << 6 | char2 & 0x3F);
            break;
          case 14:
            count += 3;
            if (count > utflen) {
              throw new UTFDataFormatException("malformed input: partial character at end");
            }
            char2 = bytearr[(count - 2)];
            int char3 = bytearr[(count - 1)];
            if (((char2 & 0xC0) != 128) || ((char3 & 0xC0) != 128)) {
              throw new UTFDataFormatException("malformed input around byte " + (count - 1));
            }
            chararr[(chararr_count++)] = (char)((c & 0xF) << 12 | (char2 & 0x3F) << 6 | (char3 & 0x3F) << 0);

            break;
          case 8:
          case 9:
          case 10:
          case 11:
          default:
            throw new UTFDataFormatException("malformed input around byte " + count);
          }
        }

        return new String(chararr, 0, chararr_count);
      }
      catch (Exception e) {
        e.printStackTrace();
      }

      return null;
    }
  }

}