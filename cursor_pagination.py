from base64 import b64decode, b64encode
from collections.abc import Sequence

from mongoengine import QuerySet, Q


class InvalidCursor(Exception):
    pass


def reverse_ordering(ordering_tuple):
    """
    Given an order_by tuple such as `('-created', 'uuid')` reverse the
    ordering and return a new tuple, eg. `('created', '-uuid')`.
    """
    def invert(x):
        return x[1:] if (x.startswith('-')) else '-' + x

    return tuple([invert(item) for item in ordering_tuple])


class CursorPage(Sequence):
    def __init__(self, items, paginator, has_next=False, has_previous=False):
        self.items = items
        self.paginator = paginator
        self.has_next = has_next
        self.has_previous = has_previous

    def __len__(self):
        return len(self.items)

    def __getitem__(self, key):
        return self.items.__getitem__(key)

    def __repr__(self):
        return '<Page: [%s%s]>' % (', '.join(repr(i) for i in self.items[:21]), ' (remaining truncated)' if len(self.items) > 21 else '')


class CursorPaginator(object):
    delimiter = '|'
    none_string = '::None'
    invalid_cursor_message = 'Invalid cursor'

    def __init__(self, queryset, ordering):
        self.queryset = queryset.order_by(*ordering)
        self.ordering = ordering

    def _apply_paginator_arguments(self, qs, first=None, last=None, after=None, before=None):
        """
        Apply first/after, last/before filtering to the queryset
        """
        from_last = last is not None
        if from_last and first is not None:
            raise ValueError('Cannot process first and last')

        if after is not None:
            qs = self.apply_cursor(after, qs, from_last=from_last)
        if before is not None:
            qs = self.apply_cursor(before, qs, from_last=from_last, reverse=True)
        if first is not None:
            qs = qs[:first + 1]
        if last is not None:
            qs = qs.order_by(*reverse_ordering(self.ordering))[:last + 1]

        return qs

    def _get_cursor_page(self, items, has_additional, first, last, after, before):
        """
        Create and return the cursor page for the given items
        """
        additional_kwargs = {}
        if first is not None:
            additional_kwargs['has_next'] = has_additional
            additional_kwargs['has_previous'] = bool(after)
        elif last is not None:
            additional_kwargs['has_previous'] = has_additional
            additional_kwargs['has_next'] = bool(before)
        return CursorPage(items, self, **additional_kwargs)

    def page(self, first=None, last=None, after=None, before=None):
        qs = self.queryset
        qs = self._apply_paginator_arguments(qs, first, last, after, before)

        qs = list(qs)
        page_size = first if first is not None else last
        items = qs[:page_size]
        if last is not None:
            items.reverse()
        has_additional = len(qs) > len(items)

        return self._get_cursor_page(items, has_additional, first, last, after, before)

    def apply_cursor(self, cursor: str, queryset: QuerySet, from_last, reverse: bool = False):
        position = self.decode_cursor(cursor)

        # Edit: The explanation below is adapted from the django implementation because
        #       MongoDB treats null values as the smallest entities during sorting.
        #       The reasoning for multi-field sorting originates from the django implementation.
        # --------------------------------------------------------------------------------------
        # this was previously implemented as tuple comparison done on postgres side
        # Assume comparing 3-tuples a and b,
        # the comparison a < b is equivalent to:
        # (a.0 < b.0) || (a.0 == b.0 && (a.1 < b.1)) || (a.0 == b.0 && a.1 == b.1 && (a.2 < b.2))
        # The expression above does not depend on short-circuit evaluation support,
        # which is usually unavailable on backend RDB

        # In order to reflect that in DB query,
        # we need to generate a corresponding WHERE-clause.

        # Suppose we have ordering ("field1", "-field2", "field3")
        # (note negation 2nd item),
        # and corresponding cursor values are ("value1", "value2", "value3"),
        # `reverse` is False.
        # Keep in mind, NULL is considered the first part of each field's order (in django implementation it's last).
        # -> everytime the pagination direction (reverse) mismatches the sort direction (is_reversed),
        # the null values need to be included explicitly in the filter.
        # In order to apply the cursor, we need to generate a following $match-clause:

        # $match: {$or: [
        #   {field1: {$gt: value1}},
        #   {field1: value1, {field2: {$or: [{$lt: value2}, {$eq: null}]}},
        #   {field1: value1, field2: value2, {field3: {$gt: value3}}
        # ]}
        #
        # We will use `__lt` lookup for `<`,
        # `__gt` for `>` and `__exact` for `=`.
        # (Using case-sensitive comparison as long as
        # cursor values come from the DB against which it is going to be compared).
        # The corresponding mongoengine ODM construct would look like:
        # filter(
        #     Q(field1__gt=value1) |
        #     Q(field1__exact=value1, (Q(field2__lt=value2 | Q(field2=None)) |
        #     Q(field1__exact=value1, field2__exact=value2, (Q(field3__lt=value3))
        # )

        # In order to remember which keys we need to compare for equality on the next iteration,
        # we need an accumulator in which we store all the previous keys.
        # When we are generating a Q object for j-th position/ordering pair,
        # our q_equality would contain equality lookups
        # for previous pairs of 0-th to (j-1)-th pairs.
        # That would allow us to generate a Q object like the following:
        # Q(f1__exact=Value(v1), f2__exact=Value(v2), ..., fj_1__exact=Value(vj_1), fj__lt=Value(vj)),
        # where the last item would depend on both "reverse" option and ordering key sign.

        filtering = Q()
        q_equality = {}

        for ordering, value in zip(self.ordering, position):
            is_reversed = ordering.startswith('-')
            o = ordering.lstrip('-')
            # mongoengine cast query parameters to field types -> NULL values are only allowed for nullable fields
            field_is_nullable = getattr(queryset._document, o).null

            if value is None:  # cursor value for the key was NULL
                if not field_is_nullable:
                    raise InvalidCursor(self.invalid_cursor_message)

                # if (forwards pagination and ascending) or (backwards pagination and descending)
                # we need to get non-Null for the key (NULL is first element in result order)
                if reverse == is_reversed:
                    q = {f"{o}__ne": None}
                    q.update(q_equality)
                    filtering |= Q(**q)

                q_equality.update({o: None})
            else:  # cursor value for the key was non-NULL
                if reverse != is_reversed:
                    comparison_key = f"{o}__lt"
                else:
                    comparison_key = f"{o}__gt"

                q = Q(**{comparison_key: value})
                # if (forwards pagination and descending) or (backwards pagination and ascending)
                # NULL values are still candidates
                if field_is_nullable and reverse != is_reversed:
                     q |= Q(**{o: None})
                filtering |= q & Q(**q_equality)

                equality_key = f"{o}__exact"
                q_equality.update({equality_key: value})

        return queryset.filter(filtering)

    def decode_cursor(self, cursor):
        try:
            orderings = b64decode(cursor.encode('ascii')).decode('utf8')
            return [ordering if ordering != self.none_string else None for ordering in orderings.split(self.delimiter)]
        except (TypeError, ValueError):
            raise InvalidCursor(self.invalid_cursor_message)

    def encode_cursor(self, position):
        encoded = b64encode(self.delimiter.join(position).encode('utf8')).decode('ascii')
        return encoded

    def position_from_instance(self, instance):
        position = []
        for order in self.ordering:
            parts = order.lstrip('-').split('__')
            attr = instance
            while parts:
                attr = getattr(attr, parts[0])
                parts.pop(0)
            if attr is None:
                position.append(self.none_string)
            else:
                position.append(str(attr))
        return position

    def cursor(self, instance):
        return self.encode_cursor(self.position_from_instance(instance))
