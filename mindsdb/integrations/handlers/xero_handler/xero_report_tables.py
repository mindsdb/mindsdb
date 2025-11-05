from typing import List, Dict, Any, Optional
import pandas as pd
from mindsdb.integrations.handlers.xero_handler.xero_tables import XeroTable


class XeroReportTable(XeroTable):
    """
    Base class for Xero Report tables.

    Xero reports have a hierarchical row/cell structure that differs from regular entity endpoints.
    This class provides common functionality for parsing report responses into flat DataFrames.
    """

    # Maximum number of period columns to support (for multi-period reports)
    MAX_PERIODS = 12

    def _parse_report_to_dataframe(self, response) -> pd.DataFrame:
        """
        Parse Xero report response into a flattened DataFrame.

        Xero reports return a hierarchical structure:
        - ReportWithRows.reports[].rows[] (recursive)
        - Each row has: row_type, title, cells[]
        - Rows can contain nested rows (sections/subsections)

        This method flattens the hierarchy into a single-level table with:
        - report_id, report_name, report_date (report metadata)
        - section, subsection, depth (hierarchy info)
        - row_title, row_type (row data)
        - account_id (from cell attributes if available)
        - period_1, period_2, ... (cell values mapped to generic columns)

        Args:
            response: ReportWithRows response from Xero API

        Returns:
            pd.DataFrame: Flattened report data
        """
        all_rows = []

        # Xero API can return multiple reports (though usually just one)
        if not hasattr(response, 'reports') or not response.reports:
            return pd.DataFrame()

        for report in response.reports:
            # Extract report metadata
            report_metadata = {
                'report_id': getattr(report, 'report_id', None),
                'report_name': getattr(report, 'report_name', None),
                'report_title': getattr(report, 'report_title', None),
                'report_type': getattr(report, 'report_type', None),
                'report_date': getattr(report, 'report_date', None),
                'updated_date_utc': getattr(report, 'updated_date_utc', None),
            }

            # Get column names from header row
            column_names = self._extract_column_names(report.rows)

            # Parse all rows recursively
            parsed_rows = self._parse_rows_recursive(
                report.rows,
                column_names=column_names,
                parent_section='',
                parent_subsection='',
                depth=0
            )

            # Add report metadata to each row
            for row_data in parsed_rows:
                row_data.update(report_metadata)

            all_rows.extend(parsed_rows)

        # Convert to DataFrame
        if not all_rows:
            return pd.DataFrame()

        df = pd.DataFrame(all_rows)

        # Ensure period columns exist (even if empty)
        for i in range(1, self.MAX_PERIODS + 1):
            period_col = f'period_{i}'
            if period_col not in df.columns:
                df[period_col] = None

        # Reorder columns to put metadata first
        metadata_cols = ['report_id', 'report_name', 'report_title', 'report_type',
                        'report_date', 'updated_date_utc', 'section', 'subsection',
                        'depth', 'row_type', 'row_title', 'account_id']
        period_cols = [f'period_{i}' for i in range(1, self.MAX_PERIODS + 1)]

        # Only include columns that exist
        ordered_cols = [col for col in metadata_cols if col in df.columns]
        ordered_cols += [col for col in period_cols if col in df.columns]

        # Add any remaining columns
        remaining_cols = [col for col in df.columns if col not in ordered_cols]
        ordered_cols += remaining_cols

        df = df[ordered_cols]

        return df

    def _extract_column_names(self, rows: List) -> List[str]:
        """
        Extract column names from the header row.

        Args:
            rows: List of ReportRow objects

        Returns:
            List[str]: Column names extracted from header cells
        """
        if not rows:
            return []

        # Find the first row with row_type='Header'
        for row in rows:
            row_type = getattr(row, 'row_type', None)
            if row_type and str(row_type).upper() in ['HEADER', 'ROW_TYPE_HEADER']:
                cells = getattr(row, 'cells', [])
                if cells:
                    return [self._get_cell_value(cell) for cell in cells]

        # If no header row found, try the first row with cells
        for row in rows:
            cells = getattr(row, 'cells', [])
            if cells:
                return [self._get_cell_value(cell) for cell in cells]

        return []

    def _parse_rows_recursive(
        self,
        rows: List,
        column_names: List[str],
        parent_section: str = '',
        parent_subsection: str = '',
        depth: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Recursively parse rows and nested rows into flat records.

        Args:
            rows: List of ReportRow objects
            column_names: List of column names from header
            parent_section: Section title from parent row
            parent_subsection: Subsection title from parent row
            depth: Current nesting depth

        Returns:
            List[Dict]: Flattened row data
        """
        parsed_rows = []

        for row in rows:
            row_type = str(getattr(row, 'row_type', 'Row')).upper()
            row_title = getattr(row, 'title', '')
            cells = getattr(row, 'cells', [])
            nested_rows = getattr(row, 'rows', [])

            # Skip header rows (already processed)
            if row_type in ['HEADER', 'ROW_TYPE_HEADER']:
                continue

            # Handle section rows (contain nested rows)
            if row_type in ['SECTION', 'ROW_TYPE_SECTION'] and nested_rows:
                # Section row - update context and recurse into nested rows
                new_section = row_title if depth == 0 else parent_section
                new_subsection = row_title if depth > 0 else parent_subsection

                parsed_rows.extend(
                    self._parse_rows_recursive(
                        nested_rows,
                        column_names=column_names,
                        parent_section=new_section,
                        parent_subsection=new_subsection,
                        depth=depth + 1
                    )
                )
            else:
                # Data row or summary row - extract cell values
                # Initialize row data
                actual_row_title = row_title

                # If row has cells, extract data
                if cells:
                    # First cell typically contains the row title/account name
                    first_cell = cells[0]
                    first_cell_value = self._get_cell_value(first_cell)

                    # Use first cell value as row title if row.title is empty/None
                    if not actual_row_title and first_cell_value:
                        actual_row_title = first_cell_value

                    row_data = {
                        'section': parent_section,
                        'subsection': parent_subsection,
                        'depth': depth,
                        'row_type': row_type,
                        'row_title': actual_row_title,
                    }

                    # Extract cell values and map to generic period columns
                    for i, cell in enumerate(cells):
                        cell_value = self._get_cell_value(cell)

                        if i == 0:
                            # First column: get account_id from cell attributes if available
                            account_id = self._get_cell_attribute(cell, 'account')
                            if account_id:
                                row_data['account_id'] = account_id
                        else:
                            # Subsequent columns: map to period columns
                            # Period numbers start at 1 (column 0 is the row title)
                            period_num = i
                            if period_num <= self.MAX_PERIODS:
                                row_data[f'period_{period_num}'] = cell_value

                    parsed_rows.append(row_data)

                # If this row has nested rows, recurse
                if nested_rows:
                    parsed_rows.extend(
                        self._parse_rows_recursive(
                            nested_rows,
                            column_names=column_names,
                            parent_section=parent_section,
                            parent_subsection=row_title,  # Current row becomes subsection
                            depth=depth + 1
                        )
                    )

        return parsed_rows

    def _get_cell_value(self, cell) -> Optional[str]:
        """
        Extract value from a report cell.

        Args:
            cell: ReportCell object

        Returns:
            str or None: Cell value
        """
        if not cell:
            return None

        value = getattr(cell, 'value', None)
        return str(value) if value is not None else None

    def _get_cell_attribute(self, cell, attribute_id: str) -> Optional[str]:
        """
        Extract a specific attribute from cell attributes.

        Args:
            cell: ReportCell object
            attribute_id: Attribute ID to extract (e.g., 'account')

        Returns:
            str or None: Attribute value
        """
        if not cell:
            return None

        attributes = getattr(cell, 'attributes', [])
        if not attributes:
            return None

        for attr in attributes:
            attr_id = getattr(attr, 'id', None)
            if attr_id == attribute_id:
                return getattr(attr, 'value', None)

        return None

    def _convert_date_parameter(self, value: Any) -> str:
        """
        Convert date parameter to Xero API format (YYYY-MM-DD).

        Args:
            value: Date value (string, datetime, etc.)

        Returns:
            str: Formatted date string
        """
        if not value:
            return None

        # If already a string in YYYY-MM-DD format, return as-is
        if isinstance(value, str):
            return value

        # If datetime object, format it
        if hasattr(value, 'strftime'):
            return value.strftime('%Y-%m-%d')

        return str(value)
