def process_string_with_skip_words(input_string, target_words, skip_words):
    words = input_string.split()
    result = []

    i = 0
    while i < len(words) - 1:
        first_word = words[i]
        second_word = words[i + 1]
        print(f"{first_word} {second_word}")

        if second_word in skip_words:
            i += 2  # Skip the current pair and move to the next one
        else:
            if first_word in target_words:
                result.append((first_word, second_word))
            i += 1  # Move to the next pair

    return result

# Example usage
input_string = "apple banana cherry apple orange apple banana"
target_words = {"apple", "banana"}
skip_words = {"cherry", "orange"}

result_pairs = process_string_with_skip_words(input_string, target_words, skip_words)
print(result_pairs)