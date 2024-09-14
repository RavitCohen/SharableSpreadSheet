using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
//Helper classes
public class Row
{
    private int myRow;
    private LinkedList<Cell> cells = new LinkedList<Cell>();

    public Row(int row, int numCol)
    {
        myRow = row;
        for (int i = 0; i < numCol; i++)
        {
            cells.AddLast(new Cell("", i));
        }
    }

    public int getmyRow() { return myRow; }
    public LinkedList<Cell> getcells() { return cells; }
    public void setmyRow(int row) { myRow = row; }
    public void setmyCells(LinkedList<Cell> cellsnew) { cells = cellsnew; }

    public List<Cell> findStrAll(string str, bool sensitive)
    {
        List<Cell> list = new List<Cell>();
        foreach (Cell cell in cells)
        {
            if (sensitive)
            {
                if (cell.getString().Equals(str))
                    list.Add(cell);
            }
            else
            {
                if (string.Equals(cell.getString(), str, StringComparison.OrdinalIgnoreCase))
                {
                    list.Add(cell);
                }
            }
        }
        return list;
    }
    public Cell findStr(string str)
    {
        foreach (Cell cell in cells)
        {
            if (cell.getString().Equals(str))
                return cell;
        }
        return null;
    }
    public int findStrCol(string str, int col1, int col2)
    {
        for (int i = col1; i <= col2; i++)
        {
            Cell c = cells.ElementAt(i);
            if (c.getString().Equals(str))
            {
                return c.getColumn();
            }
        }
        return -1;
    }
    public Cell getCell(int col)
    {
        foreach (Cell cell in cells)
        {
            if (cell.getColumn() == col)
                return cell;
        }
        return null;
    }
    public void addCell(int after)
    {
        LinkedListNode<Cell> node = null;
        for (var current = cells.First; current != null; current = current.Next)
        {
            if (current.Value.getColumn() == after)
            {
                node = current;
                break;
            }
        }
        if (node != null)
        {
            cells.AddAfter(node, new Cell("", after + 1));
            if (node.Next != null)
            {
                var current = node.Next.Next;
                while (current != null)
                {
                    current.Value.setColumn(current.Value.getColumn() + 1);
                    current = current.Next;
                }
            }
        }
    }
    public void exchangeCell(int i, int j)
    {
        string one = getCell(i).getString();
        string two = getCell(j).getString();
        getCell(i).setString(two);
        getCell(j).setString(one);

    }
    public void print()
    {
        foreach (Cell cell in cells)
        {
            Console.WriteLine($"[{getmyRow()},{cell.getColumn()}] : {cell.getString()}");
        }
    }
}

public class Cell
{
    private string myString;
    private int myColumn;

    public Cell(string str, int col)
    {
        myString = str;
        myColumn = col;
    }

    public void setString(string str) { myString = str; }
    public string getString() { return myString; }
    public int getColumn() { return myColumn; }
    public void setColumn(int col) { myColumn = col; }
}

public class SharableSpreadSheet
{
    int totalRow;
    int totalCol;
    Semaphore semaphoreSearches;
    ReaderWriterLock[] rw_mutex = new ReaderWriterLock[100];
    LinkedList<Row> spreadSheet = new LinkedList<Row>();

    public SharableSpreadSheet(int nRows, int nCols, int nUsers = -1) //Constructor to initialize a spreadsheet of size nRows x nCols.nUsers is used for setting a limit on concurrent searches, with -1 meaning no limit.
    {
        totalRow = nRows;
        totalCol = nCols;
        if (nUsers != -1)
        {
            semaphoreSearches = new Semaphore(nUsers, nUsers); // Initialize the semaphore with nUsers maximum count.
        }
        else
        {
            semaphoreSearches = new Semaphore(int.MaxValue, int.MaxValue); // No limit, allow maximum possible concurrent accesses.
        }

        for (int i = 0; i < 100; i++)
        {
            //mutexRows[i] = new Mutex();
            rw_mutex[i] = new ReaderWriterLock();
        }
        for (int i = 0; i < totalRow; i++)
        {
            spreadSheet.AddLast(new Row(i, nCols));
        }
    }

    //Helper functions
    private Row getRow(int index)
    {
        return spreadSheet.ElementAt(index);
    }
    private void addRowtoSheet(int after)
    {
        LinkedListNode<Row> node = null;
        for (var current = spreadSheet.First; current != null; current = current.Next)
        {
            if (current.Value.getmyRow() == after)
            {
                node = current;
                break;
            }
        }
        if (node != null)
        {
            spreadSheet.AddAfter(node, new Row(after + 1, totalCol));
            if (node.Next != null)
            {
                var current = node.Next.Next;
                while (current != null)
                {
                    current.Value.setmyRow(current.Value.getmyRow() + 1);
                    current = current.Next;
                }
            }
        }
    }
    private void exchangeRows(int i, int j)
    {
        LinkedList<Cell> one = getRow(i).getcells();
        LinkedList<Cell> two = getRow(j).getcells();
        getRow(i).setmyCells(two);
        getRow(j).setmyCells(one);
    }
    private Cell findCellByRC(int i, int j)
    {
        return getRow(i).getCell(j);
    }
    private List<Tuple<int, int>> findCellByStrAll(List<Row> rows, string str, bool sensitinve)
    {
        List<Tuple<int, int>> res = new List<Tuple<int, int>>();
        for (int i = 0; i < rows.Count(); i++)
        {
            List<Cell> c = rows.ElementAt(i).findStrAll(str, sensitinve);
            if (c != null)
            {
                foreach (var cell in c)
                {
                    res.Add(Tuple.Create(rows.ElementAt(i).getmyRow(), cell.getColumn()));
                }
            }
        }
        return res;
    }
    private Tuple<int, int> findCellByStr(string str, List<Row> rows)
    {
        foreach (var row in rows)
        {
            Cell c = row.findStr(str);
            int r = row.getmyRow();
            if (c != null)
            {
                return Tuple.Create(r, c.getColumn());
            }
        }
        return null;
    }
    private int findCellByStrRow(string str, int row)
    {
        Row r = getRow(row);
        Cell c = r.findStr(str);
        if (c != null)
        {
            return c.getColumn();
        }
        return -1;
    }
    private int findCellByStrCol(string str, int col, List<Row> rows)
    {
        foreach (Row row in rows)
        {
            Cell cell = row.getCell(col);
            if (cell.getString().Equals(str))
                return cell.getColumn();
        }
        return -1;
    }
    private Tuple<int, int> findCellByStrRowsCols(string str, List<Row> rows, int col1, int col2)
    {
        for (int i = 0; i < rows.Count; i++)
        {
            Row r = rows.ElementAt(i);
            int c = r.findStrCol(str, col1, col2);
            if (c != -1)
            {
                return Tuple.Create(r.getmyRow(), c);
            }
        }
        return null;
    }
    private void invalidCell(int row, int col)
    {
        if (row >= totalRow || col >= totalCol || row < 0 || col < 0)
        {
            throw new Exception($"there is not cell [{row},{col}]");
        }
    }
    private void invalidRow(int row)
    {
        if (row >= totalRow || row < 0)
        {
            throw new Exception($"there is not row {row} in spreadSheet");
        }
    }
    private void invalidCol(int col)
    {
        if (col >= totalCol || col < 0)
        {
            throw new Exception($"there is not col {col} in spreadSheet");
        }
    }

    private List<Row> relevantRows(int indexMutex)
    {
        List<Row> rows = new List<Row>();
        for (int i = 0; i < totalRow; i++)
        {
            if (i % 100 == indexMutex)
            {
                rows.Add(getRow(i));
            }
        }
        return rows;
    }

    //Methods we were asked to implement
    public string GetCell(int row, int col) //Returns the string at the specified cell[row, col].
    {
        invalidCell(row, col);
        semaphoreSearches.WaitOne();
        string res = null;
        int indexHash = row % 100;
        rw_mutex[indexHash].AcquireReaderLock(10000);
        try
        {
            Cell c = findCellByRC(row, col);
            res = c.getString();
        }
        finally
        {
            rw_mutex[indexHash].ReleaseReaderLock();
            semaphoreSearches.Release();
        }
        return res;
    }
    public void SetCell(int row, int col, string str) //Sets the string at the specified cell[row, col].
    {
        invalidCell(row, col);
        int indexHash = row % 100;
        rw_mutex[indexHash].AcquireWriterLock(10000);
        try
        {
            Cell c = findCellByRC(row, col);
            c.setString(str);
        }
        finally
        {
            rw_mutex[indexHash].ReleaseWriterLock();
        }
    }
    public Tuple<int, int> SearchString(string str) //Searches for the first cell containing the specified string and returns its position.
    {
        semaphoreSearches.WaitOne();
        Tuple<int, int> result = null;
        for (int i = 0; i < 100; i++)
        {
            rw_mutex[i].AcquireReaderLock(10000);
            try
            {
                List<Row> rows = relevantRows(i); //all the locked rows by mutex [i]
                result = findCellByStr(str, rows);
                if (result != null)
                {
                    break;
                }
            }
            finally
            {
             rw_mutex[i].ReleaseReaderLock();
            }
        }
        semaphoreSearches.Release();
        return result;
    }
    public int SearchInRow(int row, string str) //Searches for the specified string in the specified row and returns the column index.
    {
        invalidRow(row);
        int result = -1;
        int indexHash = row % 100;
        semaphoreSearches.WaitOne();
        rw_mutex[indexHash].AcquireReaderLock(10000);
        try
        {
            result = findCellByStrRow(str, row);
        }
        finally
        {
            rw_mutex[indexHash].ReleaseReaderLock();
            semaphoreSearches.Release();
        }
        return result;
    }
    public int SearchInCol(int col, string str) //Searches for the specified string in the specified column and returns the row index.
    {
        invalidCol(col);
        semaphoreSearches.WaitOne();
        int result = -1;
        for (int i = 0; i < 100; i++)
        {
                rw_mutex[i].AcquireReaderLock(10000);
            try
            {
                List<Row> rows = relevantRows(i); //all the locked rows by mutex [i]
                result = findCellByStrCol(str, col, rows);
                if (result != -1)
                {
                    break;
                }
            }
            finally
            {
                rw_mutex[i].ReleaseReaderLock();
            }
        }
        semaphoreSearches.Release();
        return result;
    }
    public Tuple<int, int> SearchInRange(int col1, int col2, int row1, int row2, string str) //Searches for the specified string within the specified range and returns its position.
    {
        invalidCol(col1);
        invalidCol(col2);
        invalidRow(row1);
        invalidRow(row2);
        semaphoreSearches.WaitOne();
        int min;
        int max;
        if (row1 < row2)
        {
            min = row1;
            max = row2;
        }
        else
        {
            min = row2;
            max = row1;
        }
        Tuple<int, int> res = null;
        SortedSet<int> mutexsLock = new SortedSet<int>();
        for (int row = min; row <= max; row++)
        {
            int indexHash = row % 100;
            mutexsLock.Add(indexHash);
        }
        foreach (int index in mutexsLock)
        {
            rw_mutex[index].AcquireReaderLock(10000);
            try
            {
                List<Row> rows = relevantRows(index); //all the locked rows by mutex [i]
                res = findCellByStrRowsCols(str, rows, col1, col2);
                if (res != null)
                {
                    break;
                }
            }
            finally
            {
                rw_mutex[index].ReleaseReaderLock();
            }
        }
        semaphoreSearches.Release();
        return res;
    }
    public Tuple<int, int>[] FindAll(string str, bool caseSensitive) //Searches for all cells containing the specified string and returns their positions.
    {
        semaphoreSearches.WaitOne();
        List<Tuple<int, int>> result = new List<Tuple<int, int>>();
        for (int i = 0; i < 100; i++)
        {
            rw_mutex[i].AcquireReaderLock(10000);
            try
            {
                List<Row> rows = relevantRows(i); //all the locked rows by mutex [i]
                result.AddRange(findCellByStrAll(rows, str, caseSensitive));
            }
            finally
            {
                rw_mutex[i].ReleaseReaderLock();
            }
        }
        semaphoreSearches.Release();
        return result.ToArray();
    }
    public void SetAll(string oldStr, string newStr, bool caseSensitive) //Replaces all occurrences of the old string with the new string according to the case sensitivity parameter.
    {
        for (int i = 0; i < 100; i++)
        {
            rw_mutex[i].AcquireWriterLock(10000);
            try
            {
                Tuple<int, int>[] res;
                List<Row> rows = relevantRows(i); //all the locked rows by mutex [i]
                res = findCellByStrAll(rows, oldStr, caseSensitive).ToArray();
                for (int j = 0; j < res.Length; j++)
                {
                    getRow(res[j].Item1).getCell(res[j].Item2).setString(newStr);
                }
            }
            finally
            {
                rw_mutex[i].ReleaseWriterLock();
            }
        }
    }
    public void ExchangeRows(int row1, int row2) //Exchanges the contents of the two specified rows.
    {
        invalidRow(row1);
        invalidRow(row2);
        int min;
        int max;
        if (row1 % 100 < row2 % 100)
        {
            min = row1 % 100;
            max = row2 % 100;
        }
        else
        {
            min = row2 % 100;
            max = row1 % 100;
        }
        if (min != max)
        {
            rw_mutex[min].AcquireWriterLock(10000);
            rw_mutex[max].AcquireWriterLock(10000);
        }
        else
        {
            rw_mutex[min].AcquireWriterLock(10000);
        }
        try
        {
            exchangeRows(row1, row2);
        }
        finally
        {
            if (min != max)
            {
                rw_mutex[min].ReleaseWriterLock();
                rw_mutex[max].ReleaseWriterLock();
            }
            else
            {
                rw_mutex[min].ReleaseWriterLock();
            }
        }
    }
    public void ExchangeCols(int col1, int col2)//Exchanges the contents of the two specified columns.
    {
        invalidCol(col1);
        invalidCol(col2);
        for (int i = 0; i < 100; i++)
        {
            rw_mutex[i].AcquireWriterLock(10000);
            try
            {
                List<Row> rows = relevantRows(i); //all the locked rows by mutex [i]
                foreach (Row row in rows)
                {
                    row.exchangeCell(col1, col2);
                }
            }
            finally
            {
                rw_mutex[i].ReleaseWriterLock();
            }
        }
    }
    public void AddRow(int row1) //Adds a row after the specified row.
    {
        invalidRow(row1);
        for (int i = 0; i < 100; i++)
        {
            rw_mutex[i].AcquireWriterLock(10000);
        }
        try
        {
          
            addRowtoSheet(row1);
            totalRow++;
        }
        finally
        {
            for (int i = 0; i < 100; i++)
            {
                rw_mutex[i].ReleaseWriterLock();
            }
        }
    }
    public void AddCol(int col1) //Adds a column after the specified column.
    {
        invalidCol(col1);
        for (int i = 0; i < 100; i++)
        {
            rw_mutex[i].AcquireWriterLock(10000);
        }
        try
        { 
            totalCol++;
            foreach (Row row in spreadSheet)
            {
                row.addCell(col1);
            }
        }
        finally
        {
            for (int i = 0; i < 100; i++)
            {
                rw_mutex[i].ReleaseWriterLock();
            }
        }
    }
    public Tuple<int, int> GetSize() //Returns the size of the spreadsheet as a tuple of number of rows and columns.
    {
        return Tuple.Create(totalRow, totalCol);
    }
    public void Print() //Prints the content of the spreadsheet, line by line.
    {
        semaphoreSearches.WaitOne();
        for (int i = 0; i < 100; i++)
        {
            rw_mutex[i].AcquireReaderLock(10000);
            try
            {
                List<Row> rows = relevantRows(i); //all the locked rows by mutex [i]
                foreach (Row row in rows)
                {
                    row.print();
                }
            }
            finally
            {
               rw_mutex[i].ReleaseReaderLock();
            }
        }
        semaphoreSearches.Release();
    }
}


    


/*
    public static void Main(string[] args)
    {
        SharableSpreadSheet sp = new SharableSpreadSheet(100,1000, -1);
        for (int r = 0; r < 100; r++)
        {
            for (int c = 0; c < 1000; c++)
            {
                sp.SetCell(r, c, $"hi{r},{c}");

            }
        }
        sp.Print();

        Console.WriteLine("------------------------");
        
        sp.SetCell(0,0, "change");
        sp.SetCell(1,1, "change");
        sp.Print();
       
        Tuple<int, int>[] k = sp.FindAll("change",true);
        for (int t = 0; t < k.Length; t++)
        {
            Console.WriteLine(k[t]);
        } 
       
        sp.ExchangeRows(0, 1);
        sp.Print();
        
        Tuple<int, int> s4 = sp.SearchString("change");
        Console.WriteLine(s4);
        
        
        sp.ExchangeCols(0, 1);
        sp.Print();
        
        Tuple<int, int> s3 = sp.SearchString("change");
        Console.WriteLine(s3);
        
        int i = sp.SearchInRow(0, "hi0,1");
        Console.WriteLine(i);

        int j = sp.SearchInCol(0, "hi0,1");
        Console.WriteLine(j);
        
        sp.Print();
        
        Tuple<int, int> h = sp.SearchInRange(0, 1, 0, 1, "hi0,0");
        Console.WriteLine(h);
        
        sp.AddRow(0);
        sp.AddCol(0);
        sp.Print();
        Console.WriteLine("------------------------");

        sp.ExchangeCols(0,1);
        sp.ExchangeRows(0,1);
        sp.Print();
        
        Tuple<int, int> h2 = sp.SearchInRange(0, 2, 0, 2, "");
        Console.WriteLine(h2);
        
        Tuple<int, int>[] o = sp.FindAll("",true);
        for (int t = 0; t < o.Length; t++)
        {
            Console.WriteLine(o[t]);
        }
        
        Tuple<int, int>[] e = sp.FindAll("HI0,0", false);
        for (int t = 0; t < e.Length; t++)
        {
            Console.WriteLine(e[t]);
        }
        
        sp.SetAll("HI0,0", "newstr", true);
        sp.Print();

        
        Tuple<int, int> u = sp.GetSize();
        Console.WriteLine(u);
        
        sp.Print();
        
    }
}
*/

