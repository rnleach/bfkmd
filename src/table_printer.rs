use std::fmt::{Display, Write};
use unicode_width::UnicodeWidthStr;

#[derive(Default, Debug)]
pub struct TablePrinter {
    title: Option<String>,
    header: Option<String>,
    footer: Option<String>,
    column_names: Vec<String>,
    columns: Vec<Vec<String>>,
    fill: String,
}

impl TablePrinter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_title<T>(self, title: T) -> Self
    where
        Option<String>: From<T>,
    {
        Self {
            title: Option::from(title),
            ..self
        }
    }

    pub fn with_header<T>(self, header: T) -> Self
    where
        Option<String>: From<T>,
    {
        Self {
            header: Option::from(header),
            ..self
        }
    }

    pub fn with_footer<T>(self, footer: T) -> Self
    where
        Option<String>: From<T>,
    {
        Self {
            footer: Option::from(footer),
            ..self
        }
    }

    pub fn with_fill<T: AsRef<str>>(self, fill_string: T) -> Self {
        Self {
            fill: fill_string.as_ref().to_owned(),
            ..self
        }
    }

    pub fn with_column<T, V>(self, col_name: T, col_vals: &[V]) -> Self
    where
        T: Display,
        V: Display,
    {
        let mut column_names = self.column_names;
        let mut columns = self.columns;

        column_names.push(format!("{}", col_name));

        let col_vals: Vec<String> = col_vals.iter().map(|v| format!("{}", v)).collect();

        columns.push(col_vals);

        Self {
            column_names,
            columns,
            ..self
        }
    }

    pub fn add_row(&mut self, row_vals: Vec<String>) {
        debug_assert!(row_vals.len() == self.columns.len());
        for (col, val) in self.columns.iter_mut().zip(row_vals.into_iter()) {
            col.push(val);
        }
    }

    pub fn print(self) -> Result<(), std::fmt::Error> {
        self.print_with_min_width(0)
    }

    pub fn print_with_min_width(self, min_width: usize) -> Result<(), std::fmt::Error> {
        let (table_width, col_widths) = self.calculate_widths(min_width);

        let builder = String::with_capacity(2000); // This should be enough

        let (left_char, right_char, builder) = self.print_the_title(table_width, builder)?;
        let (left_char, right_char, builder) =
            self.print_the_header(left_char, right_char, table_width, builder)?;
        let builder = self.print_column_names(left_char, right_char, &col_widths, builder)?;
        let builder = self.print_data_rows(&col_widths, builder)?;
        let builder = self.print_the_footer(table_width, &col_widths, builder)?;

        print!("{}", builder);

        Ok(())
    }

    /// Calculate the full table width and the widths of each column.
    fn calculate_widths(&self, min_width: usize) -> (usize, Vec<usize>) {
        let title_width = self
            .title
            .as_ref()
            .map(|title| UnicodeWidthStr::width(title.as_str()) + 2)
            .unwrap_or(0);

        let mut table_width = if min_width > title_width {
            min_width
        } else {
            title_width
        };
        let mut col_widths = vec![0; self.columns.len()];

        for (i, col_name) in self.column_names.iter().enumerate() {
            let mut width = UnicodeWidthStr::width(col_name.as_str()) + 2;
            if col_widths[i] < width {
                col_widths[i] = width;
            }

            for row in &self.columns[i] {
                width = UnicodeWidthStr::width(row.as_str()) + 2;
                if col_widths[i] < width {
                    col_widths[i] = width;
                }
            }
        }

        debug_assert!(!self.columns.is_empty(), "Must add a column.");
        let mut all_cols_width: usize =
            col_widths.iter().cloned().sum::<usize>() + col_widths.len() - 1;

        while all_cols_width < table_width {
            let min = col_widths.iter().cloned().min().unwrap();
            for width in &mut col_widths {
                if *width == min {
                    *width += 1;
                }
            }
            all_cols_width = col_widths.iter().cloned().sum::<usize>() + col_widths.len() - 1;
        }

        if all_cols_width > table_width {
            table_width = all_cols_width;
        }

        (table_width, col_widths)
    }

    /// Print the title row and return what the next left & right border chars should be.
    fn print_the_title(
        &self,
        table_width: usize,
        builder: String,
    ) -> Result<(char, char, String), std::fmt::Error> {
        let left_char: char;
        let right_char: char;
        let mut builder = builder;

        writeln!(builder)?;

        if let Some(ref title) = self.title {
            // print top border
            write!(
                builder,
                "\u{250c}{}\u{2510}\n",
                "\u{2500}".repeat(table_width)
            )?;
            // print title
            writeln!(builder, "\u{2502}{0:^1$}\u{2502}", title, table_width)?;

            // set up the border type for the next line.
            left_char = '\u{251c}';
            right_char = '\u{2524}';
        } else {
            left_char = '\u{250c}';
            right_char = '\u{2510}';
        }

        Ok((left_char, right_char, builder))
    }

    fn print_the_header(
        &self,
        left_char: char,
        right_char: char,
        table_width: usize,
        builder: String,
    ) -> Result<(char, char, String), std::fmt::Error> {
        let mut left_char = left_char;
        let mut right_char = right_char;
        let mut builder = builder;

        if let Some(ref header) = self.header {
            // print top border -  or a horizontal line
            writeln!(
                builder,
                "{}{}{}",
                left_char,
                "\u{2500}".repeat(table_width),
                right_char
            )?;
            for line in wrapper(&header, table_width) {
                writeln!(builder, "\u{2502}{0:<1$}\u{2502}", line, table_width)?;
            }

            // set up the border type for the next line.
            left_char = '\u{251c}';
            right_char = '\u{2524}';
        }

        Ok((left_char, right_char, builder))
    }

    fn print_column_names(
        &self,
        left_char: char,
        right_char: char,
        col_widths: &[usize],
        builder: String,
    ) -> Result<String, std::fmt::Error> {
        let mut builder = builder;

        // print top border above columns
        write!(builder, "{}", left_char)?;
        for &width in &col_widths[..(col_widths.len() - 1)] {
            write!(builder, "{}\u{252C}", "\u{2500}".repeat(width))?;
        }
        writeln!(
            builder,
            "{}{}",
            "\u{2500}".repeat(col_widths[col_widths.len() - 1]),
            right_char
        )?;

        // print column names
        for (name, width) in self.column_names.iter().zip(col_widths.iter()) {
            write!(builder, "\u{2502} {0:^1$} ", name, width - 2)?;
        }
        writeln!(builder, "\u{2502}")?;

        Ok(builder)
    }

    fn print_data_rows(
        &self,
        col_widths: &[usize],
        builder: String,
    ) -> Result<String, std::fmt::Error> {
        let mut builder = builder;

        // print border below column names
        write!(builder, "\u{251C}")?;
        for &width in &col_widths[..(col_widths.len() - 1)] {
            write!(builder, "{}\u{253C}", "\u{2500}".repeat(width))?;
        }
        writeln!(
            builder,
            "{}\u{2524}",
            "\u{2500}".repeat(col_widths[col_widths.len() - 1])
        )?;

        // print rows
        let num_rows = self.columns.iter().map(Vec::len).max().unwrap_or(0);
        for i in 0..num_rows {
            for (column, col_width) in self.columns.iter().zip(col_widths) {
                let val = column.get(i).unwrap_or(&self.fill);
                write!(builder, "\u{2502} {0:>1$} ", val, col_width - 2)?;
            }
            writeln!(builder, "\u{2502}")?;
        }

        Ok(builder)
    }

    fn print_the_footer(
        &self,
        table_width: usize,
        col_widths: &[usize],
        builder: String,
    ) -> Result<String, std::fmt::Error> {
        let mut builder = builder;

        // print border below data
        let (left_char, right_char) = if self.footer.is_some() {
            ('\u{251c}', '\u{2524}')
        } else {
            ('\u{2514}', '\u{2518}')
        };

        write!(builder, "{}", left_char)?;
        for &width in &col_widths[..(col_widths.len() - 1)] {
            write!(builder, "{}\u{2534}", "\u{2500}".repeat(width))?;
        }
        writeln!(
            builder,
            "{}{}",
            "\u{2500}".repeat(col_widths[col_widths.len() - 1]),
            right_char
        )?;

        if let Some(ref footer) = self.footer {
            for line in wrapper(&footer, table_width) {
                writeln!(builder, "\u{2502}{0:<1$}\u{2502}", line, table_width)?;
            }

            // print very bottom border -  or a horizontal line
            writeln!(
                builder,
                "\u{2514}{}\u{2518}",
                "\u{2500}".repeat(table_width)
            )?;
        }

        Ok(builder)
    }
}

/// Function to split the header/footers into lines
fn wrapper<'a>(text: &'a str, table_width: usize) -> Vec<&'a str> {
    let mut to_ret: Vec<&str> = vec![];

    let mut remaining = &text[..];
    while remaining.len() > table_width {
        let guess = &remaining[..table_width];

        let right_edge = guess
            .find(|c| c == '\n')
            .or_else(|| guess.rfind(char::is_whitespace))
            .unwrap_or(table_width);
        to_ret.push(&remaining[..right_edge]);
        remaining = remaining[right_edge..].trim();
    }
    to_ret.push(remaining);

    to_ret
}
