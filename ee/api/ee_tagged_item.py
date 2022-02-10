from django.db.models import Prefetch, Q
from rest_framework import serializers, viewsets

from posthog.models import Tag, TaggedItem


class EnterpriseTaggedItemSerializerMixin(serializers.Serializer):
    """
    Serializer that interacts with TaggedItem model
    """

    def get_tags(self, obj):
        if hasattr(obj, "prefetched_tags"):
            return [p.tag.name for p in obj.prefetched_tags]
        return list(obj.tags.values_list("tag__name", flat=True)) if obj.tags else []

    def set_tags(self, tags, obj):
        if not obj:
            # If the object hasn't been created yet, this method will be called again on the create method.
            return

        # Clean and dedupe tags
        deduped_tags = list(set([t.strip() for t in tags]))
        tagged_item_objects = []

        # Create tags
        for tag in deduped_tags:
            tag_instance, _ = Tag.objects.get_or_create(name=tag, team_id=obj.team_id)
            tagged_item_instance, _ = obj.tags.get_or_create(tag_id=tag_instance.id)
            tagged_item_objects.append(tagged_item_instance)

        # Delete tags that are missing
        obj.tags.exclude(tag__name__in=deduped_tags).delete()

        # Cleanup tags that aren't used by team
        Tag.objects.filter(Q(team_id=obj.team_id) & Q(taggeditems__isnull=True)).delete()

        obj.prefetched_tags = tagged_item_objects


class EnterpriseTaggedItemViewSetMixin:
    """
    Mixin that prefetches tags for list view sets.
    """

    @staticmethod
    def get_queryset_with_tags(queryset):
        return queryset.prefetch_related(
            Prefetch("tags", queryset=TaggedItem.objects.select_related("tag"), to_attr="prefetched_tags")
        )