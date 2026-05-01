using banaData.Models;
using banaData.Services;

namespace banaData
{
    public partial class Form1 : Form
    {
        private readonly SqlConnectionService _connectionService = new();
        private readonly TableMetadataService _metadataService = new();
        private readonly DataTransferService _transferService;
        private readonly ConnectionPreferencesService _preferencesService = new();

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
        private CheckedListBox _tableCheckedListBox = null!;
        private ComboBox _recordLimitComboBox = null!;
        private SplitContainer _selectionSplitContainer = null!;
        private DataGridView _tableOrderGrid = null!;
        private Button _transferButton = null!;
        private ProgressBar _progressBar = null!;
        private TextBox _logTextBox = null!;
        private readonly Dictionary<string, IReadOnlyList<ColumnMetadata>> _tableOrderCandidates = new(StringComparer.OrdinalIgnoreCase);

        private bool _sourceConnectionIsValid;
        private bool _targetConnectionIsValid;
        private bool _isLoadingSourceTables;
        private CancellationTokenSource? _transferCancellationTokenSource;

        public Form1()
        {
            _transferService = new DataTransferService(_metadataService);

            InitializeComponent();
            BuildUserInterface();
            Load += async (_, _) => await LoadConnectionPreferencesAsync();
            Shown += (_, _) => EnsureValidSplitterDistance();
        }

        protected override void OnFormClosing(FormClosingEventArgs e)
        {
            SaveConnectionPreferences();
            base.OnFormClosing(e);
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
                AutoSize = false,
                Height = 420,
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
            _tableCheckedListBox = new CheckedListBox
            {
                Dock = DockStyle.Fill,
                CheckOnClick = true,
                IntegralHeight = false
            };
            _recordLimitComboBox = CreateDropDown();
            _selectionSplitContainer = new SplitContainer
            {
                Size = new Size(1000, 400),
                Orientation = Orientation.Vertical,
                Panel1MinSize = 220,
                Panel2MinSize = 420,
                SplitterDistance = 300,
                Dock = DockStyle.Fill
            };
            _selectionSplitContainer.Resize += (_, _) => EnsureValidSplitterDistance();

            var sourceTablesGroup = new GroupBox
            {
                Text = "Source tables",
                Dock = DockStyle.Fill
            };
            sourceTablesGroup.Controls.Add(_tableCheckedListBox);
            _selectionSplitContainer.Panel1.Controls.Add(sourceTablesGroup);

            var orderRulesGroup = new GroupBox
            {
                Text = "Table order rules",
                Dock = DockStyle.Fill
            };
            _tableOrderGrid = CreateOrderRulesGrid();
            orderRulesGroup.Controls.Add(_tableOrderGrid);
            _selectionSplitContainer.Panel2.Controls.Add(orderRulesGroup);

            _recordLimitComboBox.Items.Add(new ComboBoxItem<TransferRecordLimit>("Son 1000 kayıt", TransferRecordLimit.Last1000));
            _recordLimitComboBox.Items.Add(new ComboBoxItem<TransferRecordLimit>("Son 5000 kayıt", TransferRecordLimit.Last5000));
            _recordLimitComboBox.Items.Add(new ComboBoxItem<TransferRecordLimit>("Tüm kayıtlar", TransferRecordLimit.All));
            _recordLimitComboBox.SelectedIndex = 0;

            AddLabeledControl(layout, "Source schema", _sourceSchemaComboBox);
            AddLabeledControl(layout, "Target schema", _targetSchemaComboBox);
            AddLabeledControl(layout, "Record limit", _recordLimitComboBox);
            AddWideControl(layout, _selectionSplitContainer);

            _sourceSchemaComboBox.SelectedIndexChanged += async (_, _) => await LoadSourceTablesAsync();
            _tableCheckedListBox.ItemCheck += TableCheckedListBoxOnItemCheck;
            _recordLimitComboBox.SelectedIndexChanged += (_, _) => UpdateOrderColumnState();
            _targetSchemaComboBox.SelectedIndexChanged += (_, _) => UpdateTransferButtonState();

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

        private DataGridView CreateOrderRulesGrid()
        {
            var grid = new DataGridView
            {
                Dock = DockStyle.Fill,
                AllowUserToAddRows = false,
                AllowUserToDeleteRows = false,
                AllowUserToResizeRows = false,
                MultiSelect = false,
                RowHeadersVisible = false,
                SelectionMode = DataGridViewSelectionMode.FullRowSelect,
                AutoSizeColumnsMode = DataGridViewAutoSizeColumnsMode.Fill
            };

            var tableColumn = new DataGridViewTextBoxColumn
            {
                Name = "TableName",
                HeaderText = "Table",
                FillWeight = 34,
                ReadOnly = true
            };

            var orderColumn = new DataGridViewComboBoxColumn
            {
                Name = "OrderColumn",
                HeaderText = "Order column",
                FillWeight = 46,
                FlatStyle = FlatStyle.Popup
            };

            var directionColumn = new DataGridViewComboBoxColumn
            {
                Name = "Direction",
                HeaderText = "Direction",
                FillWeight = 20,
                FlatStyle = FlatStyle.Popup
            };

            grid.Columns.Add(tableColumn);
            grid.Columns.Add(orderColumn);
            grid.Columns.Add(directionColumn);
            grid.CurrentCellDirtyStateChanged += (_, _) => UpdateTransferButtonState();
            grid.CellValueChanged += (_, _) => UpdateTransferButtonState();

            return grid;
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
            if (_isLoadingSourceTables)
            {
                return;
            }

            if (!_sourceConnectionIsValid || _sourceSchemaComboBox.SelectedItem is not string sourceSchema)
            {
                return;
            }

            if (!TryBuildConnectionSettings(true, out var settings))
            {
                return;
            }

            _isLoadingSourceTables = true;
            try
            {
                _tableCheckedListBox.Items.Clear();
                _tableOrderGrid.Rows.Clear();
                _tableOrderCandidates.Clear();

                var tables = await _metadataService.GetTablesAsync(settings, sourceSchema);
                foreach (var table in tables)
                {
                    _tableCheckedListBox.Items.Add(new ComboBoxItem<DatabaseObject>(table.Name, table), false);
                }

                AppendLog($"{sourceSchema} schema içinde {tables.Count} tablo bulundu.");
            }
            catch (Exception ex)
            {
                AppendLog($"Tablo listesi okunamadı: {ex.Message}");
            }
            finally
            {
                _isLoadingSourceTables = false;
            }

            await RebuildTableOrderOptionsAsync();
            UpdateTransferButtonState();
        }

        private async Task RebuildTableOrderOptionsAsync()
        {
            _tableOrderGrid.Rows.Clear();
            _tableOrderCandidates.Clear();

            var selectedTables = GetSelectedTables().ToArray();
            if (selectedTables.Length == 0)
            {
                UpdateOrderColumnState();
                return;
            }

            if (!_sourceConnectionIsValid ||
                _sourceSchemaComboBox.SelectedItem is not string sourceSchema ||
                !TryBuildConnectionSettings(true, out var settings))
            {
                UpdateTransferButtonState();
                return;
            }

            try
            {
                foreach (var selectedTable in selectedTables)
                {
                    var columns = await _metadataService.GetColumnsAsync(settings, sourceSchema, selectedTable.Name);
                    var candidates = columns.Where(IsGoodOrderColumnCandidate).ToArray();
                    _tableOrderCandidates[selectedTable.Name] = candidates;
                    AddOrderRuleRow(selectedTable, candidates);

                    if (candidates.Length == 0)
                    {
                        AppendLog($"Uyarı: {selectedTable.Name} tablosu için uygun sıralama kolonu bulunamadı.");
                    }
                }
            }
            catch (Exception ex)
            {
                AppendLog($"Kolon bilgileri okunamadı: {ex.Message}");
            }

            UpdateOrderColumnState();
        }

        private void TableCheckedListBoxOnItemCheck(object? sender, ItemCheckEventArgs e)
        {
            BeginInvoke(async () =>
            {
                await RebuildTableOrderOptionsAsync();
                UpdateTransferButtonState();
            });
        }

        private async Task StartTransferAsync()
        {
            if (!TryBuildTransferOptions(out var transferOptions))
            {
                return;
            }

            List<(string Table, long RowCount)> nonEmptyTargets;
            try
            {
                nonEmptyTargets = await CollectNonEmptyTargetsAsync(transferOptions);
            }
            catch (Exception ex)
            {
                var preflightMessage = $"Hedef tablo doluluk kontrolü yapılamadı: {ex.Message}";
                AppendLog(preflightMessage);
                MessageBox.Show(preflightMessage, "Ön Kontrol Hatası", MessageBoxButtons.OK, MessageBoxIcon.Error);
                return;
            }

            if (nonEmptyTargets.Count > 0)
            {
                var details = string.Join(Environment.NewLine,
                    nonEmptyTargets.Select(x => $"  - {x.Table}: {x.RowCount:N0} kayıt"));

                var answer = MessageBox.Show(
                    $"Aşağıdaki hedef tablolar boş değil:{Environment.NewLine}{Environment.NewLine}{details}{Environment.NewLine}{Environment.NewLine}" +
                    "Transfere devam edilirse PK/UC çakışmaları oluşabilir. Devam edilsin mi?",
                    "Hedef Tablo Boş Değil",
                    MessageBoxButtons.YesNo,
                    MessageBoxIcon.Warning,
                    MessageBoxDefaultButton.Button2);

                if (answer != DialogResult.Yes)
                {
                    AppendLog("Transfer iptal edildi: hedef tablolardan en az birinde mevcut kayıtlar var.");
                    return;
                }
            }

            _transferCancellationTokenSource?.Dispose();
            _transferCancellationTokenSource = new CancellationTokenSource();
            _transferButton.Enabled = false;
            _progressBar.Style = ProgressBarStyle.Marquee;

            var overallSuccess = true;
            foreach (var options in transferOptions)
            {
                AppendLog($"Tablo transferi başlıyor: {options.SourceSchema}.{options.TableName}");
                var progress = new Progress<TransferProgress>(ReportTransferProgress);
                var result = await _transferService.TransferAsync(options, progress, _transferCancellationTokenSource.Token);

                if (!result.IsSuccess)
                {
                    overallSuccess = false;
                    var body = string.IsNullOrWhiteSpace(result.Detail)
                        ? $"{options.TableName} tablosunda hata oluştu: {result.Message}"
                        : $"{options.TableName} tablosunda hata oluştu: {result.Message}\r\n\r\n--- Detay ---\r\n{result.Detail}";

                    MessageBox.Show(
                        body,
                        "Transfer Hatası",
                        MessageBoxButtons.OK,
                        MessageBoxIcon.Error);
                    break;
                }
            }

            _progressBar.Style = ProgressBarStyle.Blocks;
            _transferButton.Enabled = true;
            UpdateTransferButtonState();

            if (overallSuccess)
            {
                MessageBox.Show(
                    $"{transferOptions.Count} tablo için transfer tamamlandı.",
                    "Transfer",
                    MessageBoxButtons.OK,
                    MessageBoxIcon.Information);
            }
        }

        private async Task<List<(string Table, long RowCount)>> CollectNonEmptyTargetsAsync(
            IReadOnlyList<TransferOptions> transferOptions)
        {
            var result = new List<(string Table, long RowCount)>();
            foreach (var options in transferOptions)
            {
                var rowCount = await _metadataService.GetRowCountAsync(
                    options.TargetConnection,
                    options.TargetSchema,
                    options.TableName);

                if (rowCount > 0)
                {
                    result.Add(($"{options.TargetSchema}.{options.TableName}", rowCount));
                }
            }
            return result;
        }

        private void ReportTransferProgress(TransferProgress progress)
        {
            var detail = progress.RowsRead.HasValue || progress.RowsInserted.HasValue
                ? $" Okunan: {progress.RowsRead ?? 0}, Aktarılan: {progress.RowsInserted ?? 0}"
                : string.Empty;

            AppendLog($"{progress.Message}{detail}");

            if (progress.IsError && !string.IsNullOrWhiteSpace(progress.Detail))
            {
                AppendLog("--- Hata Detayı ---");
                foreach (var line in progress.Detail.Split('\n'))
                {
                    AppendLog(line.TrimEnd('\r'));
                }
                AppendLog("-------------------");
            }
        }

        private bool TryBuildTransferOptions(out List<TransferOptions> options)
        {
            options = [];

            if (!TryBuildConnectionSettings(true, out var sourceConnection) ||
                !TryBuildConnectionSettings(false, out var targetConnection))
            {
                return false;
            }

            if (_sourceSchemaComboBox.SelectedItem is not string sourceSchema ||
                _targetSchemaComboBox.SelectedItem is not string targetSchema ||
                _recordLimitComboBox.SelectedItem is not ComboBoxItem<TransferRecordLimit> selectedLimit)
            {
                MessageBox.Show("Source/target schema ve kayıt limiti seçilmelidir.", "Eksik Bilgi",
                    MessageBoxButtons.OK, MessageBoxIcon.Warning);
                return false;
            }

            var selectedTables = GetSelectedTables().ToArray();
            if (selectedTables.Length == 0)
            {
                MessageBox.Show("En az bir tablo seçmelisiniz.", "Eksik Bilgi",
                    MessageBoxButtons.OK, MessageBoxIcon.Warning);
                return false;
            }

            foreach (var table in selectedTables)
            {
                string? orderColumn = null;
                var direction = SortDirection.Descending;

                if (selectedLimit.Value is not TransferRecordLimit.All)
                {
                    var row = FindOrderRuleRow(table.Name);
                    if (row is null)
                    {
                        MessageBox.Show($"{table.Name} tablosu için sıralama ayarı bulunamadı.", "Eksik Bilgi",
                            MessageBoxButtons.OK, MessageBoxIcon.Warning);
                        return false;
                    }

                    var selectedOrderColumn = row.Cells["OrderColumn"].Value?.ToString();
                    if (string.IsNullOrWhiteSpace(selectedOrderColumn))
                    {
                        MessageBox.Show($"{table.Name} tablosu için sıralama kolonu seçilmelidir.", "Eksik Bilgi",
                            MessageBoxButtons.OK, MessageBoxIcon.Warning);
                        return false;
                    }

                    var selectedDirectionRaw = row.Cells["Direction"].Value?.ToString();
                    if (!Enum.TryParse<SortDirection>(selectedDirectionRaw, true, out var selectedDirection))
                    {
                        MessageBox.Show($"{table.Name} tablosu için sıralama yönü seçilmelidir.", "Eksik Bilgi",
                            MessageBoxButtons.OK, MessageBoxIcon.Warning);
                        return false;
                    }

                    orderColumn = selectedOrderColumn;
                    direction = selectedDirection;
                }

                options.Add(new TransferOptions(
                    sourceConnection,
                    targetConnection,
                    sourceSchema,
                    targetSchema,
                    table.Name,
                    selectedLimit.Value,
                    orderColumn,
                    direction));
            }

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

        private async Task LoadConnectionPreferencesAsync()
        {
            var preferences = await _preferencesService.LoadAsync();
            if (preferences is null)
            {
                return;
            }

            ApplySavedConnection(preferences.Source, true);
            ApplySavedConnection(preferences.Target, false);
        }

        private void SaveConnectionPreferences()
        {
            var preferences = new ConnectionPreferences(
                BuildSavedConnection(true),
                BuildSavedConnection(false));

            try
            {
                _preferencesService.Save(preferences);
            }
            catch (Exception ex)
            {
                AppendLog($"Bağlantı tercihleri kaydedilemedi: {ex.Message}");
            }
        }

        private SavedConnection BuildSavedConnection(bool isSource)
        {
            return new SavedConnection(
                (isSource ? _sourceServerTextBox : _targetServerTextBox).Text.Trim(),
                (isSource ? _sourceDatabaseTextBox : _targetDatabaseTextBox).Text.Trim(),
                (isSource ? _sourceIntegratedSecurityCheckBox : _targetIntegratedSecurityCheckBox).Checked,
                (isSource ? _sourceUserNameTextBox : _targetUserNameTextBox).Text.Trim(),
                (isSource ? _sourceTrustCertificateCheckBox : _targetTrustCertificateCheckBox).Checked);
        }

        private void ApplySavedConnection(SavedConnection saved, bool isSource)
        {
            var serverTextBox = isSource ? _sourceServerTextBox : _targetServerTextBox;
            var databaseTextBox = isSource ? _sourceDatabaseTextBox : _targetDatabaseTextBox;
            var integratedSecurityCheckBox = isSource ? _sourceIntegratedSecurityCheckBox : _targetIntegratedSecurityCheckBox;
            var userNameTextBox = isSource ? _sourceUserNameTextBox : _targetUserNameTextBox;
            var passwordTextBox = isSource ? _sourcePasswordTextBox : _targetPasswordTextBox;
            var trustCertificateCheckBox = isSource ? _sourceTrustCertificateCheckBox : _targetTrustCertificateCheckBox;

            serverTextBox.Text = saved.Server;
            databaseTextBox.Text = saved.Database;
            integratedSecurityCheckBox.Checked = saved.UseIntegratedSecurity;
            userNameTextBox.Text = saved.UserName;
            passwordTextBox.Text = string.Empty;
            trustCertificateCheckBox.Checked = saved.TrustServerCertificate;
            userNameTextBox.Enabled = !saved.UseIntegratedSecurity;
            passwordTextBox.Enabled = !saved.UseIntegratedSecurity;
        }

        private void UpdateOrderColumnState()
        {
            var selectedLimit = _recordLimitComboBox.SelectedItem as ComboBoxItem<TransferRecordLimit>;
            var requiresOrderColumn = selectedLimit?.Value is not TransferRecordLimit.All;

            foreach (DataGridViewRow row in _tableOrderGrid.Rows)
            {
                row.Cells["OrderColumn"].ReadOnly = !requiresOrderColumn;
                row.Cells["Direction"].ReadOnly = !requiresOrderColumn;
            }

            UpdateTransferButtonState();
        }

        private void UpdateTransferButtonState()
        {
            if (_transferButton is null)
            {
                return;
            }

            var selectedLimit = _recordLimitComboBox.SelectedItem as ComboBoxItem<TransferRecordLimit>;
            var orderColumnIsReady = selectedLimit?.Value is TransferRecordLimit.All || AreAllOrderSelectionsValid();

            _transferButton.Enabled =
                _sourceConnectionIsValid &&
                _targetConnectionIsValid &&
                _sourceSchemaComboBox.SelectedItem is not null &&
                _targetSchemaComboBox.SelectedItem is not null &&
                GetSelectedTables().Any() &&
                selectedLimit is not null &&
                orderColumnIsReady;
        }

        private bool AreAllOrderSelectionsValid()
        {
            foreach (var table in GetSelectedTables())
            {
                var row = FindOrderRuleRow(table.Name);
                if (row is null)
                {
                    return false;
                }

                if (string.IsNullOrWhiteSpace(row.Cells["OrderColumn"].Value?.ToString()))
                {
                    return false;
                }

                if (!Enum.TryParse<SortDirection>(row.Cells["Direction"].Value?.ToString(), true, out _))
                {
                    return false;
                }
            }

            return true;
        }

        private void EnsureValidSplitterDistance()
        {
            if (_selectionSplitContainer is null)
                return;

            var totalWidth = _selectionSplitContainer.ClientSize.Width;
            var min = _selectionSplitContainer.Panel1MinSize;
            var panel2Min = _selectionSplitContainer.Panel2MinSize;

            if (totalWidth <= 0 || totalWidth < min + panel2Min)
                return;

            var maxValid = totalWidth - panel2Min;
            var preferred = 300;
            var clamped = Math.Min(maxValid, Math.Max(min, preferred));

            if (_selectionSplitContainer.SplitterDistance != clamped)
                _selectionSplitContainer.SplitterDistance = clamped;
        }

        private IEnumerable<DatabaseObject> GetSelectedTables()
        {
            return _tableCheckedListBox.CheckedItems
                .OfType<ComboBoxItem<DatabaseObject>>()
                .Select(item => item.Value);
        }

        private void AddOrderRuleRow(DatabaseObject table, IReadOnlyList<ColumnMetadata> candidates)
        {
            var rowIndex = _tableOrderGrid.Rows.Add();
            var row = _tableOrderGrid.Rows[rowIndex];
            row.Cells["TableName"].Value = table.Name;

            var orderCell = new DataGridViewComboBoxCell { FlatStyle = FlatStyle.Popup };
            foreach (var candidate in candidates)
            {
                orderCell.Items.Add(candidate.Name);
            }

            var defaultColumn = SelectDefaultOrderColumn(table.Name, candidates);
            if (!string.IsNullOrWhiteSpace(defaultColumn))
            {
                orderCell.Value = defaultColumn;
            }

            var directionCell = new DataGridViewComboBoxCell { FlatStyle = FlatStyle.Popup };
            directionCell.Items.Add(SortDirection.Descending.ToString());
            directionCell.Items.Add(SortDirection.Ascending.ToString());
            directionCell.Value = SortDirection.Descending.ToString();

            row.Cells["OrderColumn"] = orderCell;
            row.Cells["Direction"] = directionCell;
        }

        private DataGridViewRow? FindOrderRuleRow(string tableName)
        {
            return _tableOrderGrid.Rows
                .Cast<DataGridViewRow>()
                .FirstOrDefault(row => string.Equals(row.Cells["TableName"].Value?.ToString(), tableName, StringComparison.OrdinalIgnoreCase));
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

        private static string? SelectDefaultOrderColumn(string tableName, IReadOnlyList<ColumnMetadata> candidates)
        {
            // 1) Primary key varsa her zaman onu kullan.
            var primaryKeyColumn = candidates
                .Where(column => column.IsPrimaryKey)
                .OrderBy(column => column.Ordinal)
                .Select(column => column.Name)
                .FirstOrDefault();

            if (!string.IsNullOrWhiteSpace(primaryKeyColumn))
            {
                return primaryKeyColumn;
            }

            // 2) PK yoksa CreateDate benzeri kolonları ara.
            var createDateColumn = candidates
                .Where(column => IsDateLikeType(column.SqlType))
                .OrderBy(column => GetCreateDateNameScore(column.Name))
                .ThenBy(column => column.Ordinal)
                .FirstOrDefault(column => GetCreateDateNameScore(column.Name) < int.MaxValue);

            if (createDateColumn is not null)
            {
                return createDateColumn.Name;
            }

            // 3) O da yoksa tablo adına benzeyen date kolonu ara (faOrder -> OrderDate).
            var tableKeyword = ExtractTableKeyword(tableName);
            if (!string.IsNullOrWhiteSpace(tableKeyword))
            {
                var relatedDateColumn = candidates
                    .Where(column => IsDateLikeType(column.SqlType))
                    .OrderBy(column => GetTableRelatedDateScore(column.Name, tableKeyword))
                    .ThenBy(column => column.Ordinal)
                    .FirstOrDefault(column => GetTableRelatedDateScore(column.Name, tableKeyword) < int.MaxValue);

                if (relatedDateColumn is not null)
                {
                    return relatedDateColumn.Name;
                }
            }

            // 4) Yine bulunamazsa mevcut genel mantığa dön.
            return candidates
                .OrderByDescending(column => IsDateLikeType(column.SqlType))
                .ThenByDescending(column => column.IsIdentity)
                .ThenByDescending(column => column.IsUnique)
                .ThenBy(column => column.Ordinal)
                .Select(column => column.Name)
                .FirstOrDefault();
        }

        private static int GetCreateDateNameScore(string columnName)
        {
            if (columnName.Equals("CreateDate", StringComparison.OrdinalIgnoreCase))
            {
                return 0;
            }

            if (columnName.Equals("CreatedDate", StringComparison.OrdinalIgnoreCase) ||
                columnName.Equals("CreationDate", StringComparison.OrdinalIgnoreCase))
            {
                return 1;
            }

            if (columnName.Equals("CreatedOn", StringComparison.OrdinalIgnoreCase) ||
                columnName.Equals("CreateDt", StringComparison.OrdinalIgnoreCase) ||
                columnName.Equals("InsertDate", StringComparison.OrdinalIgnoreCase))
            {
                return 2;
            }

            if (columnName.Contains("create", StringComparison.OrdinalIgnoreCase) &&
                columnName.Contains("date", StringComparison.OrdinalIgnoreCase))
            {
                return 3;
            }

            if (columnName.Contains("created", StringComparison.OrdinalIgnoreCase) ||
                columnName.Contains("insert", StringComparison.OrdinalIgnoreCase))
            {
                return 4;
            }

            return int.MaxValue;
        }

        private static int GetTableRelatedDateScore(string columnName, string tableKeyword)
        {
            if (columnName.Equals($"{tableKeyword}Date", StringComparison.OrdinalIgnoreCase))
            {
                return 0;
            }

            if (columnName.Contains(tableKeyword, StringComparison.OrdinalIgnoreCase) &&
                columnName.Contains("date", StringComparison.OrdinalIgnoreCase))
            {
                return 1;
            }

            return int.MaxValue;
        }

        private static string ExtractTableKeyword(string tableName)
        {
            if (string.IsNullOrWhiteSpace(tableName))
            {
                return string.Empty;
            }

            var parts = tableName
                .Split(['_', '-', ' '], StringSplitOptions.RemoveEmptyEntries)
                .SelectMany(SplitCamelCase)
                .Where(part => part.Length > 2)
                .ToArray();

            return parts.Length == 0 ? tableName : parts[^1];
        }

        private static IEnumerable<string> SplitCamelCase(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                yield break;
            }

            var start = 0;
            for (var i = 1; i < value.Length; i++)
            {
                if (char.IsUpper(value[i]) && !char.IsUpper(value[i - 1]))
                {
                    yield return value[start..i];
                    start = i;
                }
            }

            yield return value[start..];
        }

        private static bool IsDateLikeType(string sqlType)
        {
            return sqlType.Equals("date", StringComparison.OrdinalIgnoreCase) ||
                   sqlType.Equals("datetime", StringComparison.OrdinalIgnoreCase) ||
                   sqlType.Equals("datetime2", StringComparison.OrdinalIgnoreCase) ||
                   sqlType.Equals("smalldatetime", StringComparison.OrdinalIgnoreCase) ||
                   sqlType.Equals("datetimeoffset", StringComparison.OrdinalIgnoreCase);
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
