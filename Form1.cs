using banaData.Models;
using banaData.Services;

namespace banaData
{
    public partial class Form1 : Form
    {
        private readonly SqlConnectionService _connectionService = new();
        private readonly TableMetadataService _metadataService = new();
        private readonly DataTransferService _transferService;

        private TextBox _sourceServerTextBox = null!;
        private TextBox _sourceDatabaseTextBox = null!;
        private CheckBox _sourceIntegratedSecurityCheckBox = null!;
        private TextBox _sourceUserNameTextBox = null!;
        private TextBox _sourcePasswordTextBox = null!;
        private CheckBox _sourceTrustCertificateCheckBox = null!;
        private Label _sourceStatusLabel = null!;

        private TextBox _targetServerTextBox = null!;
        private TextBox _targetDatabaseTextBox = null!;
        private CheckBox _targetIntegratedSecurityCheckBox = null!;
        private TextBox _targetUserNameTextBox = null!;
        private TextBox _targetPasswordTextBox = null!;
        private CheckBox _targetTrustCertificateCheckBox = null!;
        private Label _targetStatusLabel = null!;

        private ComboBox _sourceSchemaComboBox = null!;
        private ComboBox _targetSchemaComboBox = null!;
        private ComboBox _tableComboBox = null!;
        private ComboBox _recordLimitComboBox = null!;
        private ComboBox _orderColumnComboBox = null!;
        private Button _transferButton = null!;
        private ProgressBar _progressBar = null!;
        private TextBox _logTextBox = null!;

        private bool _sourceConnectionIsValid;
        private bool _targetConnectionIsValid;
        private CancellationTokenSource? _transferCancellationTokenSource;

        public Form1()
        {
            _transferService = new DataTransferService(_metadataService);

            InitializeComponent();
            BuildUserInterface();
        }

        private void BuildUserInterface()
        {
            Text = "Data Transfer Tool";
            MinimumSize = new Size(1100, 760);
            ClientSize = new Size(1100, 760);
            StartPosition = FormStartPosition.CenterScreen;

            var rootLayout = new TableLayoutPanel
            {
                Dock = DockStyle.Fill,
                ColumnCount = 1,
                RowCount = 5,
                Padding = new Padding(12)
            };
            rootLayout.RowStyles.Add(new RowStyle(SizeType.AutoSize));
            rootLayout.RowStyles.Add(new RowStyle(SizeType.AutoSize));
            rootLayout.RowStyles.Add(new RowStyle(SizeType.AutoSize));
            rootLayout.RowStyles.Add(new RowStyle(SizeType.Percent, 100));
            rootLayout.RowStyles.Add(new RowStyle(SizeType.AutoSize));

            var connectionLayout = new TableLayoutPanel
            {
                Dock = DockStyle.Top,
                ColumnCount = 2,
                AutoSize = true
            };
            connectionLayout.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 50));
            connectionLayout.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 50));

            connectionLayout.Controls.Add(CreateConnectionGroup("Source Database", true), 0, 0);
            connectionLayout.Controls.Add(CreateConnectionGroup("Target Database", false), 1, 0);

            rootLayout.Controls.Add(connectionLayout, 0, 0);
            rootLayout.Controls.Add(CreateSelectionGroup(), 0, 1);
            rootLayout.Controls.Add(CreateTransferControls(), 0, 2);
            rootLayout.Controls.Add(CreateLogGroup(), 0, 3);

            Controls.Add(rootLayout);
            UpdateTransferButtonState();
        }

        private GroupBox CreateConnectionGroup(string title, bool isSource)
        {
            var groupBox = new GroupBox
            {
                Text = title,
                Dock = DockStyle.Fill,
                AutoSize = true,
                Padding = new Padding(10),
                Margin = new Padding(0, 0, 8, 8)
            };

            var layout = new TableLayoutPanel
            {
                Dock = DockStyle.Top,
                AutoSize = true,
                ColumnCount = 2
            };
            layout.ColumnStyles.Add(new ColumnStyle(SizeType.Absolute, 130));
            layout.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100));

            var serverTextBox = CreateTextBox();
            var databaseTextBox = CreateTextBox();
            var integratedSecurityCheckBox = new CheckBox
            {
                Text = "Windows Authentication kullan",
                Checked = true,
                Dock = DockStyle.Fill,
                AutoSize = true
            };
            var userNameTextBox = CreateTextBox();
            var passwordTextBox = CreateTextBox();
            passwordTextBox.UseSystemPasswordChar = true;
            var trustCertificateCheckBox = new CheckBox
            {
                Text = "Trust Server Certificate",
                Dock = DockStyle.Fill,
                AutoSize = true,
                Checked = true
            };
            var testButton = new Button
            {
                Text = "Test Connection",
                AutoSize = true,
                Dock = DockStyle.Left
            };
            var statusLabel = new Label
            {
                Text = "Henüz test edilmedi.",
                AutoSize = true,
                Dock = DockStyle.Fill
            };

            AddLabeledControl(layout, "Server", serverTextBox);
            AddLabeledControl(layout, "Database", databaseTextBox);
            AddWideControl(layout, integratedSecurityCheckBox);
            AddLabeledControl(layout, "User name", userNameTextBox);
            AddLabeledControl(layout, "Password", passwordTextBox);
            AddWideControl(layout, trustCertificateCheckBox);
            AddLabeledControl(layout, string.Empty, testButton);
            AddLabeledControl(layout, "Status", statusLabel);

            integratedSecurityCheckBox.CheckedChanged += (_, _) =>
            {
                userNameTextBox.Enabled = !integratedSecurityCheckBox.Checked;
                passwordTextBox.Enabled = !integratedSecurityCheckBox.Checked;
            };
            userNameTextBox.Enabled = false;
            passwordTextBox.Enabled = false;

            if (isSource)
            {
                _sourceServerTextBox = serverTextBox;
                _sourceDatabaseTextBox = databaseTextBox;
                _sourceIntegratedSecurityCheckBox = integratedSecurityCheckBox;
                _sourceUserNameTextBox = userNameTextBox;
                _sourcePasswordTextBox = passwordTextBox;
                _sourceTrustCertificateCheckBox = trustCertificateCheckBox;
                _sourceStatusLabel = statusLabel;
                testButton.Click += async (_, _) => await TestSourceConnectionAsync();
            }
            else
            {
                _targetServerTextBox = serverTextBox;
                _targetDatabaseTextBox = databaseTextBox;
                _targetIntegratedSecurityCheckBox = integratedSecurityCheckBox;
                _targetUserNameTextBox = userNameTextBox;
                _targetPasswordTextBox = passwordTextBox;
                _targetTrustCertificateCheckBox = trustCertificateCheckBox;
                _targetStatusLabel = statusLabel;
                testButton.Click += async (_, _) => await TestTargetConnectionAsync();
            }

            groupBox.Controls.Add(layout);
            return groupBox;
        }

        private GroupBox CreateSelectionGroup()
        {
            var groupBox = new GroupBox
            {
                Text = "Transfer Selection",
                Dock = DockStyle.Top,
                AutoSize = true,
                Padding = new Padding(10),
                Margin = new Padding(0, 0, 0, 8)
            };

            var layout = new TableLayoutPanel
            {
                Dock = DockStyle.Top,
                AutoSize = true,
                ColumnCount = 2
            };
            layout.ColumnStyles.Add(new ColumnStyle(SizeType.Absolute, 130));
            layout.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100));

            _sourceSchemaComboBox = CreateDropDown();
            _targetSchemaComboBox = CreateDropDown();
            _tableComboBox = CreateDropDown();
            _recordLimitComboBox = CreateDropDown();
            _orderColumnComboBox = CreateDropDown();

            _recordLimitComboBox.Items.Add(new ComboBoxItem<TransferRecordLimit>("Son 1000 kayıt", TransferRecordLimit.Last1000));
            _recordLimitComboBox.Items.Add(new ComboBoxItem<TransferRecordLimit>("Son 5000 kayıt", TransferRecordLimit.Last5000));
            _recordLimitComboBox.Items.Add(new ComboBoxItem<TransferRecordLimit>("Tüm kayıtlar", TransferRecordLimit.All));
            _recordLimitComboBox.SelectedIndex = 0;

            AddLabeledControl(layout, "Source schema", _sourceSchemaComboBox);
            AddLabeledControl(layout, "Target schema", _targetSchemaComboBox);
            AddLabeledControl(layout, "Source table", _tableComboBox);
            AddLabeledControl(layout, "Record limit", _recordLimitComboBox);
            AddLabeledControl(layout, "Order column", _orderColumnComboBox);

            _sourceSchemaComboBox.SelectedIndexChanged += async (_, _) => await LoadSourceTablesAsync();
            _tableComboBox.SelectedIndexChanged += async (_, _) => await LoadOrderColumnsAsync();
            _recordLimitComboBox.SelectedIndexChanged += (_, _) => UpdateOrderColumnState();
            _targetSchemaComboBox.SelectedIndexChanged += (_, _) => UpdateTransferButtonState();
            _orderColumnComboBox.SelectedIndexChanged += (_, _) => UpdateTransferButtonState();

            groupBox.Controls.Add(layout);
            return groupBox;
        }

        private Control CreateTransferControls()
        {
            var layout = new FlowLayoutPanel
            {
                Dock = DockStyle.Top,
                AutoSize = true,
                FlowDirection = FlowDirection.LeftToRight,
                Margin = new Padding(0, 0, 0, 8)
            };

            _transferButton = new Button
            {
                Text = "Start Transfer",
                AutoSize = true,
                Enabled = false
            };
            _transferButton.Click += async (_, _) => await StartTransferAsync();

            _progressBar = new ProgressBar
            {
                Width = 240,
                Style = ProgressBarStyle.Blocks
            };

            layout.Controls.Add(_transferButton);
            layout.Controls.Add(_progressBar);
            return layout;
        }

        private GroupBox CreateLogGroup()
        {
            var groupBox = new GroupBox
            {
                Text = "Progress",
                Dock = DockStyle.Fill,
                Padding = new Padding(10)
            };

            _logTextBox = new TextBox
            {
                Dock = DockStyle.Fill,
                Multiline = true,
                ReadOnly = true,
                ScrollBars = ScrollBars.Vertical
            };

            groupBox.Controls.Add(_logTextBox);
            return groupBox;
        }

        private async Task TestSourceConnectionAsync()
        {
            if (!TryBuildConnectionSettings(true, out var settings))
            {
                return;
            }

            _sourceStatusLabel.Text = "Test ediliyor...";
            _sourceConnectionIsValid = false;
            UpdateTransferButtonState();

            var result = await _connectionService.TestConnectionAsync(settings);
            _sourceStatusLabel.Text = result.Message;
            _sourceConnectionIsValid = result.IsSuccess;
            AppendLog($"Source: {result.Message}");

            if (result.IsSuccess)
            {
                await LoadSchemasAsync(settings, _sourceSchemaComboBox);
                await LoadSourceTablesAsync();
            }

            UpdateTransferButtonState();
        }

        private async Task TestTargetConnectionAsync()
        {
            if (!TryBuildConnectionSettings(false, out var settings))
            {
                return;
            }

            _targetStatusLabel.Text = "Test ediliyor...";
            _targetConnectionIsValid = false;
            UpdateTransferButtonState();

            var result = await _connectionService.TestConnectionAsync(settings);
            _targetStatusLabel.Text = result.Message;
            _targetConnectionIsValid = result.IsSuccess;
            AppendLog($"Target: {result.Message}");

            if (result.IsSuccess)
            {
                await LoadSchemasAsync(settings, _targetSchemaComboBox);
            }

            UpdateTransferButtonState();
        }

        private async Task LoadSchemasAsync(SqlConnectionSettings settings, ComboBox schemaComboBox)
        {
            try
            {
                schemaComboBox.Items.Clear();
                var schemas = await _metadataService.GetSchemasAsync(settings);

                foreach (var schema in schemas)
                {
                    schemaComboBox.Items.Add(schema);
                }

                schemaComboBox.SelectedItem = schemaComboBox.Items.Contains("dbo") ? "dbo" : schemaComboBox.Items.Cast<object>().FirstOrDefault();
            }
            catch (Exception ex)
            {
                AppendLog($"Schema listesi okunamadı: {ex.Message}");
            }
        }

        private async Task LoadSourceTablesAsync()
        {
            if (!_sourceConnectionIsValid || _sourceSchemaComboBox.SelectedItem is not string sourceSchema)
            {
                return;
            }

            if (!TryBuildConnectionSettings(true, out var settings))
            {
                return;
            }

            try
            {
                _tableComboBox.Items.Clear();
                _orderColumnComboBox.Items.Clear();

                var tables = await _metadataService.GetTablesAsync(settings, sourceSchema);
                foreach (var table in tables)
                {
                    _tableComboBox.Items.Add(new ComboBoxItem<DatabaseObject>(table.Name, table));
                }

                if (_tableComboBox.Items.Count > 0)
                {
                    _tableComboBox.SelectedIndex = 0;
                }

                AppendLog($"{sourceSchema} schema içinde {tables.Count} tablo bulundu.");
            }
            catch (Exception ex)
            {
                AppendLog($"Tablo listesi okunamadı: {ex.Message}");
            }

            UpdateTransferButtonState();
        }

        private async Task LoadOrderColumnsAsync()
        {
            _orderColumnComboBox.Items.Clear();

            if (!_sourceConnectionIsValid ||
                _sourceSchemaComboBox.SelectedItem is not string sourceSchema ||
                _tableComboBox.SelectedItem is not ComboBoxItem<DatabaseObject> selectedTable ||
                !TryBuildConnectionSettings(true, out var settings))
            {
                UpdateTransferButtonState();
                return;
            }

            try
            {
                var columns = await _metadataService.GetColumnsAsync(settings, sourceSchema, selectedTable.Value.Name);
                var candidates = columns.Where(IsGoodOrderColumnCandidate).ToArray();

                foreach (var column in candidates)
                {
                    var label = column.IsPrimaryKey || column.IsUnique || column.IsIdentity
                        ? $"{column.Name} ({column.SqlType}, key/identity)"
                        : $"{column.Name} ({column.SqlType})";

                    _orderColumnComboBox.Items.Add(new ComboBoxItem<string>(label, column.Name));
                }

                if (_orderColumnComboBox.Items.Count > 0)
                {
                    _orderColumnComboBox.SelectedIndex = 0;
                }
                else
                {
                    AppendLog("Uyarı: Son kayıt seçimi için kullanılabilecek net bir sıralama kolonu bulunamadı.");
                }
            }
            catch (Exception ex)
            {
                AppendLog($"Kolon bilgileri okunamadı: {ex.Message}");
            }

            UpdateOrderColumnState();
        }

        private async Task StartTransferAsync()
        {
            if (!TryBuildTransferOptions(out var options))
            {
                return;
            }

            _transferCancellationTokenSource?.Dispose();
            _transferCancellationTokenSource = new CancellationTokenSource();
            _transferButton.Enabled = false;
            _progressBar.Style = ProgressBarStyle.Marquee;

            var progress = new Progress<TransferProgress>(ReportTransferProgress);
            var result = await _transferService.TransferAsync(options, progress, _transferCancellationTokenSource.Token);

            _progressBar.Style = ProgressBarStyle.Blocks;
            _transferButton.Enabled = true;

            MessageBox.Show(result.Message, result.IsSuccess ? "Transfer" : "Transfer Hatası",
                MessageBoxButtons.OK,
                result.IsSuccess ? MessageBoxIcon.Information : MessageBoxIcon.Warning);
        }

        private void ReportTransferProgress(TransferProgress progress)
        {
            var detail = progress.RowsRead.HasValue || progress.RowsInserted.HasValue
                ? $" Okunan: {progress.RowsRead ?? 0}, Aktarılan: {progress.RowsInserted ?? 0}"
                : string.Empty;

            AppendLog($"{progress.Message}{detail}");
        }

        private bool TryBuildTransferOptions(out TransferOptions options)
        {
            options = default!;

            if (!TryBuildConnectionSettings(true, out var sourceConnection) ||
                !TryBuildConnectionSettings(false, out var targetConnection))
            {
                return false;
            }

            if (_sourceSchemaComboBox.SelectedItem is not string sourceSchema ||
                _targetSchemaComboBox.SelectedItem is not string targetSchema ||
                _tableComboBox.SelectedItem is not ComboBoxItem<DatabaseObject> selectedTable ||
                _recordLimitComboBox.SelectedItem is not ComboBoxItem<TransferRecordLimit> selectedLimit)
            {
                MessageBox.Show("Source/target schema, tablo ve kayıt limiti seçilmelidir.", "Eksik Bilgi",
                    MessageBoxButtons.OK, MessageBoxIcon.Warning);
                return false;
            }

            string? orderColumn = null;
            if (selectedLimit.Value is not TransferRecordLimit.All)
            {
                if (_orderColumnComboBox.SelectedItem is not ComboBoxItem<string> selectedOrderColumn)
                {
                    MessageBox.Show("Son kayıtları aktarabilmek için sıralama kolonu seçilmelidir.", "Eksik Bilgi",
                        MessageBoxButtons.OK, MessageBoxIcon.Warning);
                    return false;
                }

                orderColumn = selectedOrderColumn.Value;
            }

            options = new TransferOptions(
                sourceConnection,
                targetConnection,
                sourceSchema,
                targetSchema,
                selectedTable.Value.Name,
                selectedLimit.Value,
                orderColumn);

            return true;
        }

        private bool TryBuildConnectionSettings(bool isSource, out SqlConnectionSettings settings)
        {
            var serverTextBox = isSource ? _sourceServerTextBox : _targetServerTextBox;
            var databaseTextBox = isSource ? _sourceDatabaseTextBox : _targetDatabaseTextBox;
            var integratedSecurityCheckBox = isSource ? _sourceIntegratedSecurityCheckBox : _targetIntegratedSecurityCheckBox;
            var userNameTextBox = isSource ? _sourceUserNameTextBox : _targetUserNameTextBox;
            var passwordTextBox = isSource ? _sourcePasswordTextBox : _targetPasswordTextBox;
            var trustCertificateCheckBox = isSource ? _sourceTrustCertificateCheckBox : _targetTrustCertificateCheckBox;

            settings = default!;

            if (string.IsNullOrWhiteSpace(serverTextBox.Text) || string.IsNullOrWhiteSpace(databaseTextBox.Text))
            {
                MessageBox.Show("Server ve Database alanları zorunludur.", "Eksik Bilgi",
                    MessageBoxButtons.OK, MessageBoxIcon.Warning);
                return false;
            }

            if (!integratedSecurityCheckBox.Checked && string.IsNullOrWhiteSpace(userNameTextBox.Text))
            {
                MessageBox.Show("SQL Authentication için kullanıcı adı zorunludur.", "Eksik Bilgi",
                    MessageBoxButtons.OK, MessageBoxIcon.Warning);
                return false;
            }

            settings = new SqlConnectionSettings(
                serverTextBox.Text,
                databaseTextBox.Text,
                integratedSecurityCheckBox.Checked,
                userNameTextBox.Text,
                passwordTextBox.Text,
                trustCertificateCheckBox.Checked,
                EncryptConnection: true);

            return true;
        }

        private void UpdateOrderColumnState()
        {
            var selectedLimit = _recordLimitComboBox.SelectedItem as ComboBoxItem<TransferRecordLimit>;
            var requiresOrderColumn = selectedLimit?.Value is not TransferRecordLimit.All;
            _orderColumnComboBox.Enabled = requiresOrderColumn;
            UpdateTransferButtonState();
        }

        private void UpdateTransferButtonState()
        {
            if (_transferButton is null)
            {
                return;
            }

            var selectedLimit = _recordLimitComboBox.SelectedItem as ComboBoxItem<TransferRecordLimit>;
            var orderColumnIsReady = selectedLimit?.Value is TransferRecordLimit.All || _orderColumnComboBox.SelectedItem is not null;

            _transferButton.Enabled =
                _sourceConnectionIsValid &&
                _targetConnectionIsValid &&
                _sourceSchemaComboBox.SelectedItem is not null &&
                _targetSchemaComboBox.SelectedItem is not null &&
                _tableComboBox.SelectedItem is not null &&
                selectedLimit is not null &&
                orderColumnIsReady;
        }

        private void AppendLog(string message)
        {
            _logTextBox.AppendText($"[{DateTime.Now:HH:mm:ss}] {message}{Environment.NewLine}");
        }

        private static bool IsGoodOrderColumnCandidate(ColumnMetadata column)
        {
            if (column.IsComputed)
            {
                return false;
            }

            if (column.IsPrimaryKey || column.IsUnique || column.IsIdentity)
            {
                return true;
            }

            return column.SqlType.Equals("date", StringComparison.OrdinalIgnoreCase) ||
                   column.SqlType.Equals("datetime", StringComparison.OrdinalIgnoreCase) ||
                   column.SqlType.Equals("datetime2", StringComparison.OrdinalIgnoreCase) ||
                   column.SqlType.Equals("smalldatetime", StringComparison.OrdinalIgnoreCase) ||
                   column.SqlType.Equals("datetimeoffset", StringComparison.OrdinalIgnoreCase) ||
                   column.SqlType.Equals("bigint", StringComparison.OrdinalIgnoreCase) ||
                   column.SqlType.Equals("int", StringComparison.OrdinalIgnoreCase) ||
                   column.SqlType.Equals("smallint", StringComparison.OrdinalIgnoreCase) ||
                   column.SqlType.Equals("tinyint", StringComparison.OrdinalIgnoreCase);
        }

        private static TextBox CreateTextBox()
        {
            return new TextBox
            {
                Dock = DockStyle.Fill,
                Margin = new Padding(3)
            };
        }

        private static ComboBox CreateDropDown()
        {
            return new ComboBox
            {
                Dock = DockStyle.Fill,
                DropDownStyle = ComboBoxStyle.DropDownList,
                Margin = new Padding(3)
            };
        }

        private static void AddLabeledControl(TableLayoutPanel layout, string labelText, Control control)
        {
            var rowIndex = layout.RowCount++;
            layout.RowStyles.Add(new RowStyle(SizeType.AutoSize));

            layout.Controls.Add(new Label
            {
                Text = labelText,
                Dock = DockStyle.Fill,
                TextAlign = ContentAlignment.MiddleLeft,
                AutoSize = true,
                Margin = new Padding(3, 6, 3, 3)
            }, 0, rowIndex);

            layout.Controls.Add(control, 1, rowIndex);
        }

        private static void AddWideControl(TableLayoutPanel layout, Control control)
        {
            var rowIndex = layout.RowCount++;
            layout.RowStyles.Add(new RowStyle(SizeType.AutoSize));
            layout.Controls.Add(control, 0, rowIndex);
            layout.SetColumnSpan(control, 2);
        }

        private sealed class ComboBoxItem<T>
        {
            public ComboBoxItem(string text, T value)
            {
                Text = text;
                Value = value;
            }

            public string Text { get; }
            public T Value { get; }

            public override string ToString() => Text;
        }
    }
}
