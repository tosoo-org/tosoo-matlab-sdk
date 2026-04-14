function [full_data, eeg_data, actigraphy_data] = read_data(input_file)
%READ_DATA Read data from parquet file and return full data, EEG data view, and actigraphy data view.
%   [FULL_DATA, EEG_DATA, ACTIGRAPHY_DATA] = READ_DATA(INPUT_FILE)
%   reads data from a Tosoo6 parquet file and returns three tables.
%
%   Input:
%       input_file - Path to the parquet file (string or char array)
%
%   Output:
%       full_data - Complete dataset with all samples (table)
%       eeg_data - Rows where is_eeg_sample=true (table)
%       actigraphy_data - Rows where is_actigraphy_sample=true (table)
%
%   Note: The views are separate tables in MATLAB (not references like in Python)
%
%   Example:
%       [full, eeg, acti] = tosoo6.read_data('recording.tosoo6.parquet');

    % Validate input
    if ~isfile(input_file)
        error('tosoo6:read_data:FileNotFound', 'File not found: %s', input_file);
    end

    % Read the parquet file using MATLAB's built-in parquetread
    full_data = parquetread(input_file);

    % Create EEG data view - filter rows where is_eeg_sample is true
    if ismember('is_eeg_sample', full_data.Properties.VariableNames)
        eeg_data = full_data(full_data.is_eeg_sample, :);
    else
        % If column doesn't exist, return empty table with same structure
        eeg_data = full_data(false(height(full_data), 1), :);
        warning('tosoo6:read_data:NoEEGColumn', 'Column "is_eeg_sample" not found in data');
    end

    % Remove actigraphy-specific columns from EEG view
    eeg_cols_to_remove = {'is_eeg_sample', 'is_actigraphy_sample', ...
                          'acceleration_x', 'acceleration_y', 'acceleration_z'};
    eeg_cols_to_remove = eeg_cols_to_remove(ismember(eeg_cols_to_remove, eeg_data.Properties.VariableNames));
    if ~isempty(eeg_cols_to_remove)
        eeg_data(:, eeg_cols_to_remove) = [];
    end

    % Create actigraphy data view - filter rows where is_actigraphy_sample is true
    if ismember('is_actigraphy_sample', full_data.Properties.VariableNames)
        actigraphy_data = full_data(full_data.is_actigraphy_sample, :);
    else
        % If column doesn't exist, return empty table with same structure
        actigraphy_data = full_data(false(height(full_data), 1), :);
        warning('tosoo6:read_data:NoActigraphyColumn', 'Column "is_actigraphy_sample" not found in data');
    end

    % Remove EEG-specific columns from actigraphy view
    act_cols_to_remove = {'is_eeg_sample', 'is_actigraphy_sample', ...
                          'volume_gains', 'on_off_windows', 'arousal_detection', ...
                          'artifact_detection', 'min_inter_tone_time_satisfied', ...
                          'sleep_onset_time_satisfied', 'phase_condition_satisfied', ...
                          'slow_wave_detection', 'nrem_sleep_detection', ...
                          'true_stimulations_starts', 'muted_stimulations_starts'};
    % Also remove any columns starting with "eeg_"
    all_cols = actigraphy_data.Properties.VariableNames;
    eeg_prefix_cols = all_cols(startsWith(all_cols, 'eeg_'));
    act_cols_to_remove = [act_cols_to_remove, eeg_prefix_cols];
    act_cols_to_remove = act_cols_to_remove(ismember(act_cols_to_remove, actigraphy_data.Properties.VariableNames));
    if ~isempty(act_cols_to_remove)
        actigraphy_data(:, act_cols_to_remove) = [];
    end
end