#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <vector>
#include <algorithm>
#include <unordered_set>
#include <unordered_map>
#include "rapidjson/include/rapidjson/document.h"
#include "rapidjson/include/rapidjson/stringbuffer.h"
#include "rapidjson/include/rapidjson/writer.h"

#include <filesystem>
#include <chrono>
#include <fstream>
#include <iostream>

#define MAX_LINE_LENGTH 102400
#define SIMILARITY_THRESHOLD 0.7
#define MAX_COMPARE_LENGTH 128
#define BATCH_SIZE 1000

typedef struct {
    std::vector<std::string> texts;
    std::string representative;
} TextGroup;
int levenshtein_distance(const char *s1, const char *s2, int max_length) {
    int len1 = std::min(strlen(s1), (size_t)max_length);
    int len2 = std::min(strlen(s2), (size_t)max_length);
    std::vector<std::vector<int>> matrix(len1 + 1, std::vector<int>(len2 + 1));

    for (int i = 0; i <= len1; i++) {
        matrix[i][0] = i;
    }
    for (int j = 0; j <= len2; j++) {
        matrix[0][j] = j;
    }

    for (int j = 1; j <= len2; j++) {
        for (int i = 1; i <= len1; i++) {
            if (s1[i - 1] == s2[j - 1]) {
                matrix[i][j] = matrix[i - 1][j - 1];
            } else {
                int deletion = matrix[i - 1][j] + 1;
                int insertion = matrix[i][j - 1] + 1;
                int substitution = matrix[i - 1][j - 1] + 1;
                matrix[i][j] = std::min(deletion, std::min(insertion, substitution));
            }
        }
    }

    return matrix[len1][len2];
}

double similarity(const char *s1, const char *s2) {
    int len1 = strlen(s1);
    int len2 = strlen(s2);
    if (len1 == 0 || len2 == 0) {
        return len1 == len2 ? 1.0 : 0.0;
    }
    int distance = levenshtein_distance(s1, s2, MAX_COMPARE_LENGTH);
    int max_length = std::min(std::max(len1, len2), static_cast<int>(MAX_COMPARE_LENGTH));
    double sim = 1.0 - static_cast<double>(distance) / max_length;
    printf("Similarity: %s - %s = %.2f\n", s1, s2, sim);
    return sim;
}

void write_output_file(const std::string &file_name, const std::vector<TextGroup> &groups, const std::string &output_dir) {
    char output_file[1024];
    snprintf(output_file, sizeof(output_file), "%s/%s", output_dir.c_str(), file_name.c_str());

    FILE *fp_out = fopen(output_file, "w");
    if (fp_out == NULL) {
        printf("Error creating output file %s\n", output_file);
        return;
    }

    for (const auto &group : groups) {
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);

        writer.StartObject();
        writer.Key("text");
        writer.String(group.representative.c_str());
        writer.EndObject();

        fprintf(fp_out, "%s\n", buffer.GetString());
    }

    fclose(fp_out);
}

// int main(int argc, char *argv[]) {
//     if (argc != 3) {
//         printf("Usage: %s <input_dir> <output_dir>\n", argv[0]);
//         return 1;
//     }

//     char *input_dir = argv[1];
//     char *output_dir = argv[2];

//     DIR *dir = opendir(input_dir);
//     if (dir == NULL) {
//         printf("Error opening input directory\n");
//         return 1;
//     }

//     int processed_files = 0;
//     int total_files = 0;
//     struct dirent *entry;
//     while ((entry = readdir(dir)) != NULL) {
//         if (entry->d_type == DT_REG) {
//             total_files++;
//         }
//     }
//     rewinddir(dir);

//     std::unordered_map<std::string, std::unordered_set<std::string>> text_to_files;
//     std::unordered_map<std::string, std::string> text_to_longest;

//     while ((entry = readdir(dir)) != NULL) {
//         if (entry->d_type == DT_REG) {
//             processed_files++;
//             printf("Processing file %d/%d: %s\n", processed_files, total_files, entry->d_name);

//             char input_file[1024];
//             snprintf(input_file, sizeof(input_file), "%s/%s", input_dir, entry->d_name);

//             FILE *fp = fopen(input_file, "r");
//             if (fp == NULL) {
//                 printf("Error opening file %s\n", input_file);
//                 continue;
//             }

//             char line[MAX_LINE_LENGTH];
//             while (fgets(line, sizeof(line), fp)) {
//                 rapidjson::Document doc;
//                 doc.Parse(line);

//                 if (!doc.HasParseError() && doc.HasMember("text")) {
//                     std::string text = doc["text"].GetString();
//                     text_to_files[text].insert(entry->d_name);
//                     if (text_to_longest.count(text) == 0 || text.length() > text_to_longest[text].length()) {
//                         text_to_longest[text] = text;
//                     }
//                 }
//             }

//             fclose(fp);
//         }
//     }
//     rewinddir(dir);

//     std::unordered_map<std::string, std::vector<TextGroup>> file_to_groups;

//     for (const auto &pair : text_to_longest) {
//         const std::string &text = pair.second;
//         const auto &files = text_to_files[text];

//         for (const auto &file : files) {
//             bool found_group = false;
//             for (auto &group : file_to_groups[file]) {
//                 double sim = similarity(text.c_str(), group.representative.c_str());
//                 if (sim >= SIMILARITY_THRESHOLD) {
//                     group.texts.push_back(text);
//                     if (text.length() > group.representative.length()) {
//                         group.representative = text;
//                     }
//                     found_group = true;
//                     break;
//                 }
//             }

//             if (!found_group) {
//                 TextGroup new_group;
//                 new_group.texts.push_back(text);
//                 new_group.representative = text;
//                 file_to_groups[file].push_back(new_group);
//             }
//         }
//     }

//     while ((entry = readdir(dir)) != NULL) {
//         if (entry->d_type == DT_REG) {
//             write_output_file(entry->d_name, file_to_groups[entry->d_name], output_dir);
//         }
//     }

//     closedir(dir);

//     return 0;
// }
void process_batch(std::vector<std::string>& batch, std::unordered_set<std::string>& all_representatives, std::ofstream& out_file, std::ofstream& log_file, int& processed_lines, std::chrono::steady_clock::time_point start_time) {
    std::vector<TextGroup> groups;

    for (const std::string& line : batch) {
        rapidjson::Document doc;
        doc.Parse(line.c_str());

        if (!doc.HasParseError() && doc.HasMember("text")) {
            std::string text = doc["text"].GetString();

            bool found = false;
            for (auto& group : groups) {
                if (similarity(text.c_str(), group.representative.c_str()) >= SIMILARITY_THRESHOLD) {
                    group.texts.push_back(text);
                    if (text.length() > group.representative.length()) {
                        group.representative = text;
                    }
                    found = true;
                    break;
                }
            }

            if (!found) {
                TextGroup newGroup;
                newGroup.texts.push_back(text);
                newGroup.representative = text;
                groups.push_back(newGroup);
            }
        }
    }

    for (const auto& group : groups) {
        if (all_representatives.insert(group.representative).second) {
            rapidjson::StringBuffer buffer;
            rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);

            writer.StartObject();
            writer.Key("representative");
            writer.String(group.representative.c_str());
            writer.Key("texts");
            writer.StartArray();
            for (const auto& text : group.texts) {
                writer.String(text.c_str());
            }
            writer.EndArray();
            writer.EndObject();

            out_file << buffer.GetString() << std::endl;
            
            // Log similar texts and their representative
            if (group.texts.size() > 1) {
                log_file << "Representative: " << group.representative << std::endl;
                for (const auto& text : group.texts) {
                    if (text != group.representative) {
                        log_file << " - Similar: " << text << std::endl;
                    }
                }
                log_file << std::endl;
            }
        }
    }

    processed_lines += batch.size();
    auto current_time = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(current_time - start_time).count();
    std::cout << "🏎️  Processed " << processed_lines << " (Elapsed time: " << elapsed << "s)" << std::endl;
    std::cout.flush(); // Ensure that the progress is output immediately
}
void disable_output() {
#ifdef _WIN32
    freopen("nul", "w", stdout);
    freopen("nul", "w", stderr);
#else
    freopen("/dev/null", "w", stdout);
    freopen("/dev/null", "w", stderr);
#endif
}

int main(int argc, char *argv[]) {
        disable_output();

    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <input_file.jsonl>" << std::endl;
        return 1;
    }

    std::string input_file = argv[1];
    std::filesystem::path input_path(input_file);
    std::string output_dir = "./output";
    std::string output_file_name = "dedup_" + input_path.filename().string();
    std::string output_file = output_dir + "/" + output_file_name;
    std::string log_file_name = "similar_texts.txt";
    std::string log_file_path = output_dir + "/" + log_file_name;

    // Ensure output directory exists
    std::filesystem::create_directories(output_dir);

    std::ifstream file(input_file);
    if (!file.is_open()) {
        std::cerr << "Error opening file: " << input_file << std::endl;
        return 1;
    }

    // Count the total number of lines in the input file
    int total_lines = std::count(std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>(), '\n');
    file.clear();  // Clear EOF flag
    file.seekg(0); // Move to the beginning of the file
    std::cout << "🔥 Total lines in input file: " << total_lines << std::endl;

    std::ofstream out_file(output_file);
    if (!out_file.is_open()) {
        std::cerr << "Error opening output file: " << output_file << std::endl;
        return 1;
    }

    std::ofstream log_file(log_file_path);
    if (!log_file.is_open()) {
        std::cerr << "Error opening log file: " << log_file_path << std::endl;
        return 1;
    }

    std::string line;
    std::vector<std::string> batch;
    std::unordered_set<std::string> all_representatives;
    int processed_lines = 0;

    // Start the clock
    auto start_time = std::chrono::steady_clock::now();

    while (std::getline(file, line)) {
        batch.push_back(line);
        if (batch.size() >= BATCH_SIZE) {
            process_batch(batch, all_representatives, out_file, log_file, processed_lines, start_time);
            batch.clear();
        }
    }

    if (!batch.empty()) {
        process_batch(batch, all_representatives, out_file, log_file, processed_lines, start_time);
    }

    // Close the output file and count the number of lines
    out_file.close();
    std::ifstream final_output(output_file);
    int output_lines = std::count(std::istreambuf_iterator<char>(final_output), std::istreambuf_iterator<char>(), '\n');
    std::cout << "🌈 Total lines in output file: " << output_lines << std::endl;
    std::cout << "🗑️  Reduction: " << total_lines - output_lines << " lines." << std::endl;

    auto end_time = std::chrono::steady_clock::now();
    auto total_elapsed = std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time).count();
    std::cout << "⏱️  Total processing time: " << total_elapsed << "s." << std::endl;

    return 0;
}
