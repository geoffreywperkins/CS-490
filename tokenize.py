import string



def collect_chars(query, allowed_characters):
	chars = []

	for char in query:
		if char not in allowed_characters:
			break
		chars.append(char)

	return ''.join(chars)


def remove_leading_whitespace(query, tokens):
	whitespace = collect_chars(query, string.whitespace)
	return query[len(whitespace):]


def remove_word(query, tokens):
	word = collect_chars(query, string.ascii_letters + '_' + string.digits)

	if word == 'NULL':
		tokens.append(None)
	else:
		tokens.append(word)

	return query[len(word):]


def remove_text(query, tokens):
	assert query[0] == "'"
	query = query[1:]
	end_index = query.find("'")
	text = query[:end_index]
	tokens.append(text)
	query = query[end_index + 1:]


def remove_num(query, tokens):
	query = remove_integer(query, tokens)

	if query[0] == '.':
		whole_str = tokens.pop()
		query = query[1:]
		query = remove_integer(query, tokens)
		frac_str = tokens.pop()
		float_str = whole_str + '.' + frac_str
		tokens.append(float(float_str))

	else:
		tin_str = tokens.pop()
		tokens.append(int(int_str))

	return query


def remove_integer(query, tokens):
	int_str = collect_chars(query, string.digits)
	tokens.append(int_str)
	return query[len(int_str):]


def tokenize(query):
	tokens = []

	while query:
		print("Query: ", query)
		print("Tokens: ", tokens)

		old_query = query

		if query[0] is in string.whitespace:
			remove_leading_whitespace(query, tokens)
			continue

		if query[0] is in string.ascii_letters + '_':
			query = remove_word(query, tokens)
			continue

		if query[0] is in '(),':
			tokens.append(query[0])
			query = query[1:]
			continue

		if query[0] == "'":
			query = remove_text(query, tokens)
			continue

		if query[0] is in string.digits:
			query = remove_num(query, tokens)
			continue

		if len(query) == len(old_query):
			raise AssertionError("Query didn't get shorter")

	return tokens