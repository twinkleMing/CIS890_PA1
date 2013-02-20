

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.WritableComparable;

public class ArrayListWritableComparable<E extends WritableComparable> extends ArrayList<E>
  implements WritableComparable
{
  private static final long serialVersionUID = 1L;

  public ArrayListWritableComparable()
  {
  }

  public ArrayListWritableComparable(ArrayList<E> array)
  {
    super(array);
  }

  public void readFields(DataInput in)
    throws IOException
  {
    clear();

    int numFields = in.readInt();
    if (numFields == 0)
      return;
    String className = in.readUTF();
    try
    {
      Class c = Class.forName(className);
      for (int i = 0; i < numFields; i++) {
        WritableComparable obj = (WritableComparable)c.newInstance();
        obj.readFields(in);
        add((E) obj);
      }
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void write(DataOutput out)
    throws IOException
  {
    out.writeInt(size());
    if (size() == 0)
      return;
    WritableComparable obj = (WritableComparable)get(0);

    out.writeUTF(obj.getClass().getCanonicalName());

    for (int i = 0; i < size(); i++) {
      obj = (WritableComparable)get(i);
      if (obj == null) {
        throw new IOException("Cannot serialize null fields!");
      }
      obj.write(out);
    }
  }

  public int compareTo(Object obj)
  {
    ArrayListWritableComparable that = (ArrayListWritableComparable)obj;

    for (int i = 0; i < size(); i++)
    {
      if (i >= that.size()) {
        return 1;
      }

      Comparable thisField = (Comparable)get(i);

      Comparable thatField = (Comparable)that.get(i);

      if (thisField.equals(thatField))
      {
        if (i == size() - 1) {
          if (size() > that.size()) {
            return 1;
          }
          if (size() < that.size())
            return -1;
        }
      }
      else {
        return thisField.compareTo(thatField);
      }
    }

    return 0;
  }

  public String toString()
  {
    StringBuffer sb = new StringBuffer();
    sb.append("[");
    for (int i = 0; i < size(); i++) {
      if (i != 0)
        sb.append(", ");
      sb.append(get(i));
    }
    sb.append("]");

    return sb.toString();
  }

}