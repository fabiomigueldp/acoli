from django.urls import include, path
from rest_framework.routers import DefaultRouter

from api.views import AcolyteProfileViewSet, AssignmentViewSet, MassInstanceViewSet

router = DefaultRouter()
router.register("acolytes", AcolyteProfileViewSet)
router.register("masses", MassInstanceViewSet)
router.register("assignments", AssignmentViewSet)

urlpatterns = [
    path("", include(router.urls)),
]

